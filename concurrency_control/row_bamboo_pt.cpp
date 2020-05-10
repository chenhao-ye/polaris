#include "row.h"
#include "txn.h"
#include "row_bamboo_pt.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_bamboo_pt::init(row_t * row) {
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
  // record first conflicting write to rollback
  fcw = NULL;
#if LATCH == LH_SPINLOCK
  latch = new pthread_spinlock_t;
  pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#elif LATCH == LH_MUTEX
  latch = new pthread_mutex_t;
  pthread_mutex_init(latch, NULL);
#else
  latch = new mcslock();
#endif
  blatch = false;
}

inline 
void Row_bamboo_pt::lock(BBLockEntry * en) {
  if (g_thread_cnt > 1) {
    if (g_central_man)
      glob_manager->lock_row(_row);
    else {
#if LATCH == LH_SPINLOCK
      pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
      pthread_mutex_lock( latch );
#else
      latch->acquire(en->m_node);
#endif
    }
  }
}

inline 
void Row_bamboo_pt::unlock(BBLockEntry * en) {
  if (g_thread_cnt > 1) {
    if (g_central_man)
      glob_manager->release_row(_row);
    else {
#if LATCH == LH_SPINLOCK
      pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
      pthread_mutex_unlock( latch );
#else
      latch->release(en->m_node)
#endif
    }
  }
}

RC Row_bamboo_pt::lock_get(lock_t type, txn_man * txn, Access * access) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt, access);
}


RC Row_bamboo_pt::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids,
    int &txncnt , Access * access) {
  assert (CC_ALG == BAMBOO);
  // move malloc out
  BBLockEntry * entry = get_entry(access);
  entry->type = type;
  // a linked list storing entries to return in the end

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
  starttime = get_sys_clock();
#endif

  BBLockEntry * to_reset = NULL;
  // each thread has at most one owner of a lock
  assert(owner_cnt <= g_thread_cnt);
  // each thread has at most one waiter
  assert(waiter_cnt < g_thread_cnt);
  assert(retired_cnt < g_thread_cnt);

  // assign a ts if ts == 0
  if (txn->get_ts() == 0)
    txn->set_next_ts(1);

  // 1. set txn to abort in owners and retired
  RC rc = RCOK;
  ts_t ts = txn->get_ts();

  // check if can grab directly
  if (retired_cnt == 0 && owner_cnt == 0 && waiter_cnt == 0) {
    entry->next = NULL;
    QUEUE_PUSH(owners, owners_tail, entry);
    entry->status = LOCK_OWNER;
    owner_cnt++;
    txn->lock_ready = true;
    unlock(entry);
    return rc;
  }

  // check retired and wound conflicted
  BBLockEntry * en;
  en = retired_head;
  while (en != NULL) {
    // self assigned, if conflicted, assign a number
    if (rc == RCOK && conflict_lock(en->type, type) && (en->txn->get_ts() > ts))
      rc = WAIT;
    if (rc == WAIT && en->txn->get_ts() > ts) {
      if (txn->wound_txn(en->txn) == COMMITED) {
        rc = Abort;
        bring_next(NULL);
        unlock(entry);
        return_entry(entry);
        return rc;
      }
      en = remove_descendants(en);
    } else {
      en = en->next;
    }
  }

  // check owners
  BBLockEntry * prev = NULL;
  en = owners;
  while (en != NULL) {
    // self assigned, if conflicted, assign a number
    if (rc == RCOK && conflict_lock(en->type, type) && (en->txn->get_ts() > ts))
      rc = WAIT;
    if (rc == WAIT && en->txn->get_ts() > ts) {
      if (txn->wound_txn(en->txn) == COMMITED) {
        rc = Abort;
        bring_next(NULL);
        unlock(entry);
        return_entry(entry);
        return rc;
      }
      to_reset = en;
      QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
      en = en->next;
      return_entry(to_reset);
    } else {
      prev = en;
      en = en->next;
    }
  }

  // 2. insert into waiters and bring in next waiter
  en = waiters_head;
  while (en != NULL) {
    if (ts < en->txn->get_ts())
      break;
    en = en->next;
  }
  if (en) {
    LIST_INSERT_BEFORE(en, entry);
    if (en == waiters_head)
      waiters_head = entry;
  } else {
    LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
  }
  entry->status = LOCK_WAITER;
  waiter_cnt ++;
  rc = WAIT;

  // 3. if brought txn in owner, return acquired lock
  if (bring_next(txn)) {
    rc = RCOK;
  } else
    txn->lock_ready = false; // wait in waiters

  // 5. move reads to retired if RETIRE_READ=false
  if (owners && (waiter_cnt > 0) && (owners->type == LOCK_SH)) {
    // if retire turned on and share lock is the owner
    // move to retired
    BBLockEntry * to_retire = NULL;
    while (owners) {
      to_retire = owners;
      LIST_RM(owners, owners_tail, to_retire, owner_cnt);
      to_retire->next=NULL;
      to_retire->prev=NULL;
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
      to_retire->status = LOCK_RETIRED;
      retired_cnt++;
    }
    if (bring_next(txn)) {
      rc = RCOK;
    }
  }
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - starttime);
#endif
  unlock(entry);
  return rc;
}

RC Row_bamboo_pt::lock_retire(void * addr) {
  BBLockEntry * entry = (BBLockEntry *) addr;
#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug4, get_sys_clock() - starttime);
    starttime = get_sys_clock();
#endif
  RC rc = RCOK;
  // 1. find entry in owner and remove
  if (entry->status == LOCK_OWNER) {
    // rm from owners
    LIST_RM(owners, owners_tail, entry, owner_cnt);
    entry->next = NULL;
    entry->prev = NULL;
    //assert(entry->txn->get_ts() != 0);
    // try to add to retired
    UPDATE_RETIRE_INFO(entry, retired_tail);
    LIST_PUT_TAIL(retired_head, retired_tail, entry);
    entry->status = LOCK_RETIRED;
    retired_cnt++;
    // make dirty data globally visible
    if (entry->type == LOCK_EX)
      entry->access->orig_row->copy(entry->access->data);
  } else {
    // may be is aborted
    //assert(txn->status == ABORTED);
    assert(entry->status == LOCK_DROPPED);
    rc = Abort;
  }
  if (owner_cnt == 0)
    bring_next(NULL);

#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug5, get_sys_clock() - starttime);
    starttime = get_sys_clock();
#endif
  unlock(entry);
  return rc;
}

RC Row_bamboo_pt::lock_release(void * addr, RC rc) {
  auto entry = (BBLockEntry *) addr;
#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug6, get_sys_clock() - starttime);
    starttime = get_sys_clock();
#endif
  // if in retired
  if (entry->status == LOCK_RETIRED) {
    rm_from_retired(entry, rc == Abort);
    return_entry(entry);
  } else if (entry->status == LOCK_OWNER) {
    LIST_RM(owners, owners_tail, entry, owner_cnt);
    return_entry(entry);
  } else if (entry->status == LOCK_WAITER) {
    LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
    return_entry(entry);
  } else {
    // not found in retired, need to make globally visible if rc = commit
    if (rc == Commit)
      entry->access->orig_row->copy(entry->access->data);
  }
  if (owner_cnt == 0) {
    bring_next(NULL);
  }
  // WAIT - done releasing with is_abort = true
  // FINISH - done releasing with is_abort = false
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug7, get_sys_clock() - starttime);
#endif
  unlock(entry);
  return RCOK;
}

inline 
void Row_bamboo_pt::rm_from_retired(BBLockEntry * en, bool is_abort) {
  fcw = NULL;
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
}

inline 
bool Row_bamboo_pt::bring_next(txn_man * txn) {
  bool has_txn = false;
  BBLockEntry * entry;
  // If any waiter can join the owners, just do it!
  while (waiters_head) {
    if ((owner_cnt == 0) || (!conflict_lock(owners->type, waiters_head->type))) {
      LIST_GET_HEAD(waiters_head, waiters_tail, entry);
      waiter_cnt --;
      // add to onwers
      LIST_PUT_TAIL(owners, owners_tail, entry);
      entry->status = LOCK_OWNER;
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

inline 
bool Row_bamboo_pt::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)
    return true;
  else
    return false;
}


inline 
bool Row_bamboo_pt::conflict_lock_entry(BBLockEntry * l1, BBLockEntry * l2) {
  if (l1 == NULL || l2 == NULL)
    return false;
  return conflict_lock(l1->type, l2->type);
}


inline 
BBLockEntry * Row_bamboo_pt::get_entry(Access * access) {
  //BBLockEntry * entry = (BBLockEntry *) mem_allocator.alloc(sizeof(BBLockEntry), _row->get_part_id());
  return (BBLockEntry *) access->lock_entry;
}

inline 
void Row_bamboo_pt::return_entry(BBLockEntry * entry) {
  //mem_allocator.free(entry, sizeof(BBLockEntry));
  entry->next = NULL;
  entry->prev = NULL;
  entry->type = LOCK_NONE;
  entry->status = LOCK_DROPPED;
  entry->is_cohead = false;
  entry->delta = true;
}

inline 
void Row_bamboo_pt::update_entry(BBLockEntry * en) {
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

inline
BBLockEntry * Row_bamboo_pt::remove_descendants(BBLockEntry * en, txn_man *
txn) {
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
