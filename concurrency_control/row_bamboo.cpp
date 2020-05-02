#include "row.h"
#include "txn.h"
#include "row_bamboo.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_bamboo::init(row_t * row) {
  _row = row;
  // owners is a double linked list, each entry/node contains info like lock type, prev/next
  owners = NULL;
  owners_tail = NULL;
  // waiter is a double linked list. two ptrs to the linked lists
  waiters_head = NULL;
  waiters_tail = NULL;
  // retired is a double linked list
  retired_head = NULL;
  retired_tail = NULL;
  owner_cnt = 0;
  waiter_cnt = 0;
  retired_cnt = 0;
  // local timestamp
  local_ts = -1;
  txn_ts = 0;
  // record first conflicting write to rollback
  fcw = NULL;

#if SPINLOCK
  latch = new pthread_spinlock_t;
  pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#else
  latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
#endif
  blatch = false;
}

inline
void Row_bamboo::lock() {
  if (g_thread_cnt > 1) {
    if (g_central_man)
      glob_manager->lock_row(_row);
    else {
#if SPINLOCK
      pthread_spin_lock( latch );
#else
      pthread_mutex_lock( latch );
#endif
    }
  }
}

inline
void Row_bamboo::unlock() {
  if (g_thread_cnt > 1) {
    if (g_central_man)
      glob_manager->release_row(_row);
    else {
#if SPINLOCK
      pthread_spin_unlock( latch );
#else
      pthread_mutex_unlock( latch );
#endif
    }
  }
}


RC Row_bamboo::lock_get(lock_t type, txn_man * txn, Access * access) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt, access);
}

RC Row_bamboo::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int
&txncnt, Access * access) {
  // allocate an lock entry
  assert (CC_ALG == BAMBOO);
  BBLockEntry * to_insert;
  to_insert = get_entry();
  to_insert->access = access;
  to_insert->txn = txn;
  to_insert->type = type;

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock();
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
  starttime = get_sys_clock();
#endif

  // 1. set txn to abort in owners and retired
  RC rc = WAIT;
  RC status = RCOK;
  // if unassigned, grab or assign the largest possible number
  local_ts = -1;
  ts_t ts = txn->get_ts();
  txn_ts = ts;
  if (ts == 0) {
    // test if can grab the lock without assigning priority
    if ((waiter_cnt == 0) &&
        (retired_cnt == 0 || (!conflict_lock(retired_tail->type, type) && retired_tail->is_cohead)) &&
        (owner_cnt == 0 || !conflict_lock(owners->type, type)) ) {
      // add to owners directly
      txn->lock_ready = true;
      LIST_PUT_TAIL(owners, owners_tail, to_insert);
      owner_cnt++;
      unlock();
      return RCOK;
    }
    // else has to assign a priority and add to waiters first
    // assert(retired_cnt + owner_cnt != 0);
    // heuristic to batch assign ts:
    //int batch_n_ts = retired_cnt + owner_cnt + 1;

    int batch_n_ts = 1;
    if ( waiter_cnt == 0 ) {
      if (retired_tail && (retired_tail->txn->get_ts() == 0)) {
        batch_n_ts += retired_cnt;
      }
      batch_n_ts += owner_cnt;
    }
    //local_ts = txn->set_next_ts(retired_cnt + owner_cnt + 1);
    local_ts = txn->set_next_ts(batch_n_ts);
    if (local_ts != 0) {
      // if != 0, already booked n ts.
      txn_ts = local_ts;
      local_ts = local_ts - batch_n_ts + 1;
      //assert(txn->get_ts() != local_ts); // make sure did not change pointed addr
    } else {
      // if == 0, fail to assign, oops, self has an assigned number anyway
      ts = txn->get_ts();
      txn_ts = ts;
    }
  }

  // 2. wound conflicts
  // 2.1 check retired
  fcw = NULL;
  status = wound_conflict(type, txn, ts, true, status);
  if (status == Abort) {
    rc = Abort;
    if (owner_cnt == 0)
      bring_next(NULL);
    unlock();
    return_entry(to_insert);
    return rc;
  }
#if BB_OPT_RAW
  else if (status == FINISH) {
    // RAW conflict, need to read its orig_data by making a read copy
    access->data->copy(fcw->access->orig_data);
    // insert before writer
    LIST_INSERT_BEFORE_CH(retired_head, fcw, to_insert);
    retired_cnt++;
    fcw = NULL;
    txn->lock_ready = true;
    unlock();
    return FINISH;
  }
#endif

  // 2.2 check owners
  status = wound_conflict(type, txn, ts, false, status);
  if (status == Abort) {
    rc = Abort;
    if (owner_cnt == 0)
      bring_next(NULL);
    unlock();
    return_entry(to_insert);
    return rc;
  }
#if BB_OPT_RAW
  else if (status == FINISH) {
    // RAW conflict, need to read its orig_data by making a read copy
    access->data->copy(fcw->access->orig_data);
    // append to the end of retired
    ASSERT(!retired_tail || retired_tail->is_cohead);
    LIST_PUT_TAIL(retired_head, retired_tail, to_insert);
    retired_cnt++;
    fcw = NULL;
    txn->lock_ready = true;
    unlock();
    return FINISH;
  }
#endif

  // 2. insert into waiters and bring in next waiter
  to_insert->txn = txn;
  to_insert->type = type;
  BBLockEntry * en = waiters_head;
  while (en != NULL) {
    if (txn_ts < en->txn->get_ts())
      break;
    en = en->next;
  }
  if (en) {
    LIST_INSERT_BEFORE_CH(waiters_head, en, to_insert);
  } else {
    LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
  }
  waiter_cnt ++;
  txn->lock_ready = false;

  // 3. bring next available to owner in case both are read
  if (bring_next(txn)) {
    rc = RCOK;
  }

  // 4. retire read directly
  if (owners && (waiter_cnt > 0) && (owners->type == LOCK_SH)) {
    // if retire turned on and share lock is the owner
    // move to retired
    BBLockEntry * to_retire = NULL;
    while (owners) {
      to_retire = owners;
      LIST_RM(owners, owners_tail, to_retire, owner_cnt);
      to_retire->next = NULL;
      to_retire->prev = NULL;
      // try to add to retired
      if (retired_tail) {
        if (conflict_lock(retired_tail->type, to_retire->type)) {
          // conflict with tail -> increment barrier for sure
          // default is_cohead = false
          to_retire->delta = true;
          to_retire->txn->increment_commit_barriers();
        } else {
          // not conflict with tail ->increment if is not head
          to_retire->is_cohead = retired_tail->is_cohead;
          if (!to_retire->is_cohead)
            to_retire->txn->increment_commit_barriers();
        }
      } else {
        to_retire->is_cohead = true;
      }
      LIST_PUT_TAIL(retired_head, retired_tail, to_retire);
      retired_cnt++;
    }
    if (owner_cnt == 0 && bring_next(txn)) {
      rc = RCOK;
    }
  }

#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - starttime);
#endif
  unlock();
  return rc;
}

RC Row_bamboo::lock_retire(txn_man * txn) {


#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock();
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug4, get_sys_clock() - starttime);
    starttime = get_sys_clock();
#endif

  RC rc = RCOK;
  // 1. find entry in owner and remove
  BBLockEntry * entry = owners;
  while (entry) {
    if (entry->txn == txn)
      break;
    entry = entry->next;
  }
  if (entry) {
    // rm from owners
    LIST_RM(owners, owners_tail, entry, owner_cnt);
    entry->next = NULL;
    entry->prev = NULL;
    //assert(entry->txn->get_ts() != 0);
    // try to add to retired
    if (retired_tail) {
      if (conflict_lock(retired_tail->type, entry->type)) {
        // conflict with tail -> increment barrier for sure
        // default is_cohead = false
        entry->delta = true;
        entry->txn->increment_commit_barriers();
      } else {
        // not conflict with tail ->increment if is not head
        entry->is_cohead = retired_tail->is_cohead;
        if (!entry->is_cohead)
          entry->txn->increment_commit_barriers();
      }
    } else {
      entry->is_cohead = true;
    }
    LIST_PUT_TAIL(retired_head, retired_tail, entry);
    retired_cnt++;
    // make dirty data globally visible
    if (entry->type == LOCK_EX)
      entry->access->orig_row->copy(entry->access->data);
  } else {
    // may be is aborted
    //assert(txn->status == ABORTED);
    rc = Abort;
  }
  if (owner_cnt == 0)
    bring_next(NULL);

#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug5, get_sys_clock() - starttime);
    starttime = get_sys_clock();
#endif
  unlock();
  return rc;
}

RC Row_bamboo::lock_release(txn_man * txn, RC rc) {
#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock();
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug6, get_sys_clock() - starttime);
    starttime = get_sys_clock();
#endif
  BBLockEntry * en = NULL;
  // Try to find the entry in the retired
  if (!rm_if_in_retired(txn, rc == Abort)) {
    // Try to find the entry in the owners
    en = owners;
    while (en) {
      if (en->txn == txn)
        break;
      en = en->next;
    }
    if (en) {
      // rm from owners
      LIST_RM(owners, owners_tail, en, owner_cnt);
    } else {
      // not found in owner or retired, try waiters
      en = waiters_head;
      while(en) {
        if (en->txn == txn) {
          LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
          break;
        }
        en = en->next;
      }
    }
    // not found in retired, need to make globally visible if rc = commit
    if (en && (rc == Commit))
      en->access->orig_row->copy(en->access->data);
  }
  if (owner_cnt == 0) {
    bring_next(NULL);
  }
  // WAIT - done releasing with is_abort = true
  // FINISH - done releasing with is_abort = false
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug7, get_sys_clock() - starttime);
#endif
  unlock();
  if (en)
    return_entry(en);
  return RCOK;
}

inline bool Row_bamboo::rm_if_in_retired(txn_man * txn, bool is_abort) {
  fcw = NULL;
  BBLockEntry * en = retired_head;
  while(en) {
    if (en->txn == txn) {
      if (is_abort) {
        en->txn->lock_abort = true;
        CHECK_ROLL_BACK(en)
        en = remove_descendants(en, txn);
      } else {
        assert(txn->status == COMMITED);
        update_entry(en);
        LIST_RM(retired_head, retired_tail, en, retired_cnt);
        return_entry(en);
      }
      return true;
    } else
      en = en->next;
  }
  return false;
}

inline bool
Row_bamboo::bring_next(txn_man * txn) {
  bool has_txn = false;
  BBLockEntry * entry;
  // If any waiter can join the owners, just do it!
  while (waiters_head) {
    if ((owner_cnt == 0) || (!conflict_lock(owners->type, waiters_head->type))) {
      LIST_GET_HEAD(waiters_head, waiters_tail, entry);
      waiter_cnt --;
      // add to onwers
      LIST_PUT_TAIL(owners, owners_tail, entry);
      owner_cnt ++;
      entry->txn->lock_ready = true;
      if (txn == entry->txn) {
        has_txn = true;
      }
    } else
      break;
  }
  ASSERT((owners == NULL) == (owner_cnt == 0));
  return has_txn;
}


inline bool Row_bamboo::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)
    return true;
  else
    return false;
}

inline bool Row_bamboo::conflict_lock_entry(BBLockEntry * l1,
    BBLockEntry * l2) {
  if (l1 == NULL || l2 == NULL)
    return false;
  return conflict_lock(l1->type, l2->type);
}

BBLockEntry * Row_bamboo::get_entry() {
  BBLockEntry * entry = (BBLockEntry *) mem_allocator.alloc(sizeof(BBLockEntry), _row->get_part_id());
  entry->prev = NULL;
  entry->next = NULL;
  entry->delta = false;
  entry->is_cohead = false;
  entry->txn = NULL;
  entry->access = NULL;
  return entry;
}

void Row_bamboo::return_entry(BBLockEntry * entry) {
  mem_allocator.free(entry, sizeof(BBLockEntry));
}

inline bool Row_bamboo::wound_txn(BBLockEntry * en, txn_man * txn, bool check_retired) {

  if (txn->status == ABORTED)
    return false;
  if (en->txn->set_abort() != ABORTED)
    return false;
  if (check_retired) {
    CHECK_ROLL_BACK(en);
    en = remove_descendants(en, txn);
  } else {
    LIST_RM(owners, owners_tail, en, owner_cnt);
    return_entry(en);
  }
  return true;
}

inline RC Row_bamboo::wound_conflict(lock_t type, txn_man * txn, ts_t ts,
    bool check_retired, RC status) {
  BBLockEntry * en;
  BBLockEntry * to_reset;
  if (check_retired)
    en = retired_head;
  else
    en = owners;
  bool recheck = false;
  int checked_cnt = 0;
  while (en) {
    checked_cnt++;
    recheck = false;
    ts_t en_ts = en->txn->get_ts();
    if (ts != 0) {
      // self assigned, if conflicted, assign a number
      if (status == RCOK && conflict_lock(en->type, type) &&
          ((en_ts > txn_ts) || (en_ts == 0))) {
        status = WAIT;
#if BB_OPT_RAW
        if (type == LOCK_SH) {
          // RAW conflict. read orig_data of the entry
          fcw = en;
          return FINISH;
        }
#endif
      }
      if (status == WAIT) {
        if ((en_ts > ts) || (en_ts == 0)) {
          // has conflict.
          to_reset = en;
          en = en->prev;
          if (!wound_txn(to_reset, txn, check_retired))
            return Abort;
          if (en)
            en = en->next;
          else {
            if (check_retired)
              en = retired_head;
            else
              en = owners;
          }
        } else {
          en = en->next;
        }
      } else {
        en = en->next;
      }
    } else {
      // if already commited, abort self
      if (en->txn->status == COMMITED) {
        en = en->next;
        continue;
      }
      // self unassigned, if not assigned, assign a number;
      if (en_ts == 0) {
        // if already commited, abort self
        if (en->txn->status == COMMITED) {
          en = en->next;
          continue;
        }
        assert(local_ts < txn_ts);
        if (!en->txn->atomic_set_ts(local_ts)) { // it has a ts already
          recheck = true;
        } else {
          en_ts = local_ts;
          local_ts++;
        }
      }
      //if (!recheck && (en->txn->get_ts() > txn->get_ts())) {
      if (!recheck && (en_ts > txn_ts)) {
        to_reset = en;
        en = en->prev;
        if (!wound_txn(to_reset, txn, check_retired))
          return Abort;
        // if has previous
        if (en)
          en = en->next;
        else {
          if (check_retired)
            en = retired_head;
          else
            en = owners;
        }
      } else {
        if (!recheck)
          en = en->next;
        else
          checked_cnt--;
      }
    }
  }
  return status;
}

inline BBLockEntry *
Row_bamboo::remove_descendants(BBLockEntry * en, txn_man * txn) {
#if DEBUG_CS_PROFILING
  uint32_t abort_cnt = 1;
  uint32_t abort_try = 1;
#endif
  assert(en != NULL);
  BBLockEntry * next = NULL;
  BBLockEntry * prev = en->prev;
  // 1. remove self, set iterator to next entry
  lock_t type = en->type;
  bool conflict_with_owners = conflict_lock_entry(en, owners);
  next = en->next;
  if (type == LOCK_SH) {
    // update entry and no need to remove descendants!
    update_entry(en);
    LIST_RM(retired_head, retired_tail, en, retired_cnt);
    return_entry(en);
    if (prev)
      return prev->next;
    else
      return retired_head;
  }
  //update_entry(en); // no need to update as any non-cohead needs to be aborted, coheads will not be aborted
  LIST_RM(retired_head, retired_tail, en, retired_cnt);
  return_entry(en);
  en = next;
  // 2. remove next conflict till end
  // 2.1 find next conflict
  while(en && (!conflict_lock(type, en->type))) {
    en = en->next;
  }
  // 2.2 remove dependendees,
  // NOTE: only the first wounded/aborted txn needs to be rolled back
  if (en == NULL) {
    // no dependendees, if conflict with owner, need to empty owner
    if (conflict_with_owners) {
      ABORT_ALL_OWNERS()
    } // else, nothing to do
  } else {
    // abort till end and empty owners
    LIST_RM_SINCE(retired_head, retired_tail, en);
    while(en) {
      next = en->next;
      en->txn->set_abort();
      CHECK_ROLL_BACK(en);
      retired_cnt--;
      return_entry(en);
      en = next;
    }
    ABORT_ALL_OWNERS()
  }
  assert(!retired_head || retired_head->is_cohead);
#if DEBUG_CS_PROFILING
  // debug9: sum of all lengths of chains; debug 10: time of cascading aborts; debug2: max chain
    if (abort_cnt > 1) {
        INC_STATS(0, debug2, 1);
        INC_STATS(0, debug9, abort_cnt); // out of all aborts, how many are cascading aborts (have >= 1 dependency)
    }
    // max length of aborts
    if (abort_cnt > stats._stats[txn->get_thd_id()]->debug11)
        stats._stats[txn->get_thd_id()]->debug11 = abort_cnt;
    // max length of depedency
    if (abort_try > stats._stats[txn->get_thd_id()]->debug10)
        stats._stats[txn->get_thd_id()]->debug10 = abort_try;
#endif
  if (prev)
    return prev->next;
  else
    return retired_head;
}


inline void Row_bamboo::update_entry(BBLockEntry * en) {
  BBLockEntry * entry;
  if (en->prev) {
    if (en->next) {
      if (en->delta && !en->next->delta) // WR(1)R(0)
        en->next->delta = true;
    } else {
      // has no next, nothing needs to be updated
    }
  } else {
    // has no previous, en = head
    if (en->next) {
      // has next entry
      // en->next->is_cohead = true;
      if (!en->next->is_cohead) {
        en->next->delta = false;
        entry = en->next;
        while(entry && (!entry->delta)) {
          assert(!entry->is_cohead);
          entry->is_cohead = true;
          entry->txn->decrement_commit_barriers();
          entry = entry->next;
        }
      } // else (R)RR, no changes
      assert(en->next->is_cohead);
    } else {
      // has no next entry, never mind
    }
  }
}
