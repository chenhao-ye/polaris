#include "row.h"
#include "txn.h"
#include "row_bamboo_pt.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_bamboo_pt::init(row_t * row) {
  _row = row;
  // owners is a double linked list, each entry/node contains info like lock type, prev/next
  owners = NULL;
  // waiter is a double linked list. two ptrs to the linked lists
  waiters_head = NULL;
  waiters_tail = NULL;
  // retired is a double linked list
  retired_head = NULL;
  retired_tail = NULL;
  waiter_cnt = 0;
  retired_cnt = 0;
  // record first conflicting write to rollback
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
      latch->release(en->m_node);
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

  // allocate lock entry
  BBLockEntry * to_insert = get_entry(access);
  to_insert->type = type;
  // helper
  RC rc = RCOK;
  BBLockEntry * en;
#if DEBUG_ABORT_LENGTH
  txn->abort_chain = 0;
#endif

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(to_insert);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), time_get_latch, get_sys_clock() - starttime);
  starttime = get_sys_clock();
#endif

  // assign ts if does not have one
  ts_t ts = txn->get_ts();
  if (ts == 0) {
    txn->set_next_ts(1);
    ts = txn->get_ts();
  }

  if (type == LOCK_SH) {
    bool retired_has_write = (retired_tail && (retired_tail->type==LOCK_EX ||
        !retired_tail->is_cohead));
    if (!retired_has_write) {
      if (!owners) {
        // append to retired
        ADD_TO_RETIRED_TAIL(to_insert);
      } else {
        if (ts > owners->txn->get_ts()) {
          ADD_TO_WAITERS(en, to_insert);
          goto final;
        } else {
#if BB_OPT_RAW
          access->data->copy(owners->access->orig_data);
          ADD_TO_RETIRED_TAIL(to_insert);
          rc = FINISH;
#else
          // wound owner
          TRY_WOUND_PT(owners, to_insert);
          ABORT_ALL_OWNERS(en);
          // add to waiters
          ADD_TO_WAITERS(en, to_insert);
          if (bring_next(txn)) {
            txn->lock_ready = true;
            rc = RCOK;
          }
#endif
        }
      }
    } else {
      if (!owners || (owners->txn->get_ts() > ts)) {
#if BB_OPT_RAW
        // go through retired and insert
        en = retired_head;
        while (en) {
          if (en->type == LOCK_EX && (en->txn->get_ts() > ts))
            break;
          en = en->next;
        }
        if (en) {
          access->data->copy(en->access->orig_data);
          INSERT_TO_RETIRED(to_insert, en);
        } else {
          if (owners)
            access->data->copy(owners->access->orig_data);
          ADD_TO_RETIRED_TAIL(to_insert);
        }
        rc = FINISH;
#else
        assert(false);
#endif
      } else {
          // add to waiter
          ADD_TO_WAITERS(en, to_insert);
          goto final;
      }
    }
  } else {
    // LOCK_EX
    if (!owners || (owners->txn->get_ts() > ts)) {
      // go through retired and wound
      en = retired_head;
      while (en) {
        if (en->txn->get_ts() > ts) {
          TRY_WOUND_PT(en, to_insert);
          // abort descendants
          en = rm_from_retired(en, true, txn);
        } else
          en = en->next;
      }
      if (owners) {
        // abort owners as well
        TRY_WOUND_PT(owners, to_insert);
        return_entry(owners);
        owners = NULL;
      }
    }
    ADD_TO_WAITERS(en, to_insert);
    if (bring_next(txn)) {
      rc = RCOK;
    } else {
      goto final;
    }
  }
  to_insert->txn->lock_ready = true;
  final:
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif
  unlock(to_insert);
  return rc;
}

RC Row_bamboo_pt::lock_retire(void * addr) {
  BBLockEntry * entry = (BBLockEntry *) addr;
  ASSERT(entry->type == LOCK_EX);
#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  uint64_t endtime = get_sys_clock();
  INC_STATS(entry->txn->get_thd_id(), time_retire_latch, endtime - starttime);
  starttime = endtime;
#endif
  RC rc = RCOK;
  // 1. find entry in owner and remove
  if (entry->status == LOCK_OWNER) {
    RETIRE_ENTRY(entry);
    // make dirty data globally visible
    if (entry->type == LOCK_EX)
      entry->access->orig_row->copy(entry->access->data);
  } else {
    // may be is aborted: assert(txn->status == ABORTED);
    assert(entry->status == LOCK_DROPPED);
    rc = Abort;
  }
  bring_next(NULL);

#if DEBUG_CS_PROFILING
  INC_STATS(entry->txn->get_thd_id(), time_retire_cs, get_sys_clock() -
  starttime);
#endif
  unlock(entry);
  return rc;
}

RC Row_bamboo_pt::lock_release(void * addr, RC rc) {
  BBLockEntry * entry = (BBLockEntry *) addr;
#if DEBUG_ABORT_LENGTH
  entry->txn->abort_chain = 0;
#endif
#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  uint64_t endtime = get_sys_clock();
  INC_STATS(entry->txn->get_thd_id(), time_release_latch, endtime- starttime);
  starttime = endtime;
#endif
  // if in retired
  if (entry->status == LOCK_RETIRED) {
    rm_from_retired(entry, rc == Abort, entry->txn);
  } else if (entry->status == LOCK_OWNER) {
    owners = NULL;
    // not found in retired, need to make globally visible if rc = commit
    if (rc == RCOK && (entry->type == LOCK_EX))
       entry->access->orig_row->copy(entry->access->data);
  } else if (entry->status == LOCK_WAITER) {
    LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
  } else {
  }
  return_entry(entry);
  if (!owners) {
    bring_next(NULL);
  }
  // WAIT - done releasing with is_abort = true
  // FINISH - done releasing with is_abort = false
#if DEBUG_CS_PROFILING
  INC_STATS(entry->txn->get_thd_id(), time_release_cs, get_sys_clock() -
  starttime);
#endif
  unlock(entry);
#if DEBUG_ABORT_LENGTH
  if (entry->txn->abort_chain > 0)
    UPDATE_STATS(entry->txn->get_thd_id(), abort_length, entry->txn->abort_chain);
#endif
  return RCOK;
}

/*
 * return next lock entry in the retired list after removing en (and its
 * descendants if is_abort = true)
 */
inline 
BBLockEntry * Row_bamboo_pt::rm_from_retired(BBLockEntry * en, bool is_abort, txn_man * txn) {
  if (is_abort && (en->type == LOCK_EX)) {
    CHECK_ROLL_BACK(en); // roll back only for the first-conflicting-write
    en->txn->lock_abort = true;
    en = remove_descendants(en, txn);
    return en;
  } else {
    BBLockEntry * next = en->next;
    update_entry(en);
    LIST_RM(retired_head, retired_tail, en, retired_cnt);
    return_entry(en);
    return next;
  }
}

inline 
bool Row_bamboo_pt::bring_next(txn_man * txn) {
  bool has_txn = false;
  BBLockEntry * entry;
  // If any waiter can join the owners, just do it!
  while (waiters_head) {
    if (!owners || (!conflict_lock(owners->type, waiters_head->type))) {
      LIST_GET_HEAD(waiters_head, waiters_tail, entry);
      waiter_cnt --;
      if (entry->type == LOCK_EX) {
        // add to onwers
        owners = entry;
        entry->status = LOCK_OWNER;
      } else {
        // add to retired
        ADD_TO_RETIRED_TAIL(entry);
      }
      entry->txn->lock_ready = true;
      if (txn == entry->txn) {
        has_txn = true;
      }
    } else
      break;
  }
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
  BBLockEntry * entry = (BBLockEntry *) access->lock_entry;
  entry->txn->lock_ready = false;
  entry->txn->lock_abort = false;
  entry->next = NULL;
  entry->prev = NULL;
  entry->status = LOCK_DROPPED;
  entry->is_cohead = false;
  entry->delta = true;
  return entry;
}

inline 
void Row_bamboo_pt::return_entry(BBLockEntry * entry) {
  //mem_allocator.free(entry, sizeof(BBLockEntry));
  entry->next = NULL;
  entry->prev = NULL;
  entry->status = LOCK_DROPPED;
}

inline 
void Row_bamboo_pt::update_entry(BBLockEntry * entry) {
  if (!entry->next)
    return; // nothing to update
  if (entry->type == LOCK_SH) {
    // cohead: no need to update
    // delta: update next entry only
    if (entry->prev) {
      entry->next->delta = entry->prev->type == LOCK_EX;
    }
    return;
  }
  // if entry->type == LOCK_EX, has to be co-head and txn commits, otherwise
  // abort will not call this
  ASSERT(entry->is_cohead);
  BBLockEntry * en = entry->next;
  if (entry->prev) {
    // prev can only be reads
    // cohead: whatever it is, it becomes NEW cohead and decrement barrier
    // delta: set only the immediate next
    en->delta = false;
    // R(W-committed)RRR -- yes for entry->next->next, i.e. en->next
    // R(W-committed)RWR -- yes
    // R(W-committed)WRR -- no
    // R(W-committed)WWR -- no
    while (en && !(en->delta)) {
      en->is_cohead = true;
      en->txn->decrement_commit_barriers();
      en = en->next;
    }
  } else {
    // cohead: whatever it is becomes cohead
    // delta: whatever it is delta is false
    entry->delta = false;
    do { // RRRRW
      en->is_cohead = true;
      en->txn->decrement_commit_barriers();
      en = en->next;
    } while(en && !(en->delta));
  }
}

inline
BBLockEntry * Row_bamboo_pt::remove_descendants(BBLockEntry * en, txn_man *
txn) {
  // en->type must be LOCK_EX, which conflicts with everything after it.
  // including owners
  assert(en->type == LOCK_EX);
  BBLockEntry * prev = en->prev;
  BBLockEntry * to_return;
  // abort till end, no need to update barrier as set abort anyway
  LIST_RM_SINCE(retired_head, retired_tail, en);
  while(en) {
    en->txn->set_abort();
    to_return = en;
    retired_cnt--;
#if DEBUG_ABORT_LENGTH
    txn->abort_chain++;
#endif
    en = en->next;
    return_entry(to_return);
  }
  // empty owners
  if (owners) {
#if DEBUG_ABORT_LENGTH
    txn->abort_chain++;
#endif
    owners->txn->set_abort();
    return_entry(owners);
    owners = NULL;
  }
  assert(!retired_head || retired_head->is_cohead);

  if (prev)
    return prev->next;
  else
    return retired_head;
}
