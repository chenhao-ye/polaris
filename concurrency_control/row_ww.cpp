#include "row.h"
#include "txn.h"
#include "row_ww.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_ww::init(row_t * row) {
  _row = row;
  // owners is a single linked list, each entry/node contains info like lock type, prev/next
  owners = NULL;
  // waiter is a double linked list. two ptrs to the linked lists
  waiters_head = NULL;
  waiters_tail = NULL;
  owner_cnt = 0;
  waiter_cnt = 0;

#if LATCH == LH_SPINLOCK
  latch = new pthread_spinlock_t;
	pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#elif LATCH == LH_MUTEX
  latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#else
  latch = new mcslock();
#endif

  lock_type = LOCK_NONE;
  blatch = false;
}

// taking the latch
void Row_ww::lock(txn_man * txn) {
    if (likely(g_thread_cnt > 1)) {
            if (unlikely(g_central_man))
                glob_manager->lock_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_lock( latch );
#else
                latch->acquire(txn->mcs_node);
#endif
            }
    }
};

// release the latch
void Row_ww::unlock(txn_man * txn) {
        if (likely(g_thread_cnt > 1)) {
            if (unlikely(g_central_man))
                glob_manager->release_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_unlock( latch );
#else
                latch->release(txn->mcs_node);
#endif
            }
        }
};

RC Row_ww::lock_get(lock_t type, txn_man * txn, Access * access) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt, access);
}

RC Row_ww::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int&txncnt,
    Access * access) {
  assert (CC_ALG == WOUND_WAIT);
  RC rc;
  LockEntry * entry = get_entry(access);
  LockEntry * en;
#if PF_ABORT 
  txn->abort_chain = 0;
#endif
#if PF_CS
  uint64_t starttime = get_sys_clock();
#endif
#if PF_MODEL
  INC_STATS(txn->get_thd_id(), lock_acquire_cnt, 1);
#endif
  lock(entry->txn);
  COMPILER_BARRIER
#if PF_CS
  uint64_t endtime = get_sys_clock();
  INC_STATS(txn->get_thd_id(), time_get_latch, endtime - starttime);
  starttime = endtime;
#endif

  // each thread has at most one owner of a lock
  assert(owner_cnt <= g_thread_cnt);
  // each thread has at most one waiter
  assert(waiter_cnt < g_thread_cnt);

  if (owner_cnt == 0) {
    // if owner is empty, grab the lock
    entry->type = type;
    entry->txn = txn;
    STACK_PUSH(owners, entry);
    entry->status = LOCK_OWNER;
    owner_cnt ++;
    lock_type = type;
    txn->lock_ready = true;
    rc = RCOK;
  } else {
    en = owners;
    LockEntry * prev = NULL;
    while (en != NULL) {
      if (en->txn->get_ts() > txn->get_ts() && conflict_lock(lock_type, type)) {
        if (!txn->wound_txn(en->txn)){
          // txn to wound is already pre-committed or comitted
          if (owner_cnt == 0)
            bring_next();
          rc = Abort;
          entry->status = LOCK_DROPPED;
          goto final;
        }
        // remove from owner
        if (prev)
          prev->next = en->next;
        else {
          if (owners == en)
            owners = en->next;
        }
        // free en
        en->status = LOCK_DROPPED;
        // update count
        owner_cnt--;
        if (owner_cnt == 0)
          lock_type = LOCK_NONE;
      } else {
        prev = en;
      }
      en = en->next;
    }
    // insert to wait list, the waiter list is always in timestamp order
    entry->txn = txn;
    entry->type = type;
    en = waiters_head;
    while ((en != NULL) && (txn->get_ts() > en->txn->get_ts()))
      en = en->next;
    if (en) {
      LIST_INSERT_BEFORE(en, entry);
      if (en == waiters_head)
        waiters_head = entry;
    } else
      LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
    entry->status = LOCK_WAITER;
    waiter_cnt ++;
    txn->lock_ready = false;
    rc = WAIT;
    bring_next();

    // if brought in owner return acquired lock
    en = owners;
    while(en){
      if (en->txn == txn) {
        rc = RCOK;
        break;
      }
      en = en->next;
    }
  }

  final:
#if PF_CS
  INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif
  COMPILER_BARRIER
  unlock(entry->txn);
#if PF_MODEL
  if (rc == RCOK)
      INC_STATS(txn->get_thd_id(), lock_directly_cnt, 1);
#endif
#if PF_ABORT 
  if (txn->abort_chain > 0) {
    UPDATE_STATS(txn->get_thd_id(), max_abort_length, txn->abort_chain);
    INC_STATS(txn->get_thd_id(), cascading_abort_times, 1);
    INC_STATS(txn->get_thd_id(), abort_length, txn->abort_chain);
  }
#endif
  return rc;
}


RC Row_ww::lock_release(LockEntry * entry) {
#if PF_CS
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry->txn);
  COMPILER_BARRIER
#if PF_CS
  uint64_t endtime = get_sys_clock();
  INC_STATS(entry->txn->get_thd_id(), time_release_latch, endtime - starttime);
  starttime = endtime;
#endif

  // Try to find the entry in the owners
  if (entry->status == LOCK_OWNER) {
    LockEntry * en = owners;
    LockEntry * prev = NULL;
    while ((en != NULL) && (en != entry)) {
      prev = en;
      en = en->next;
    }
    if (prev)
      prev->next = en->next;
    else{
      if (owners == en)
        owners = en->next;
    }
    owner_cnt --;
    if (owner_cnt == 0)
      lock_type = LOCK_NONE;
  } else if (entry->status == LOCK_WAITER) {
    LIST_REMOVE(entry);
    if (entry == waiters_head)
      waiters_head = entry->next;
    if (entry == waiters_tail)
      waiters_tail = entry->prev;
    waiter_cnt --;
  }
  bring_next();
  ASSERT((owners == NULL) == (owner_cnt == 0));
#if PF_CS
  INC_STATS(entry->txn->get_thd_id(), time_release_cs, get_sys_clock() -
  starttime);
#endif
  unlock(entry->txn);
  COMPILER_BARRIER
#if PF_ABORT 
  if (entry->txn->abort_chain > 0)
    UPDATE_STATS(entry->txn->get_thd_id(), abort_length, entry->txn->abort_chain);
#endif
  return RCOK;
}

bool Row_ww::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)
    return true;
  else
    return false;
}

inline 
LockEntry * Row_ww::get_entry(Access * access) {
  //LockEntry * entry = (LockEntry *) mem_allocator.alloc(sizeof(LockEntry),
  // _row->get_part_id());
  #if CC_ALG == WOUND_WAIT
  LockEntry * entry = access->lock_entry;
  entry->next = NULL;
  entry->prev = NULL;
  entry->status = LOCK_DROPPED;
  return entry;
  #else
  return NULL;
  #endif
}

inline 
void Row_ww::return_entry(LockEntry * entry) {
  entry->status = LOCK_DROPPED;
  entry->next = NULL;
  entry->prev = NULL;
  entry->type = LOCK_NONE;
  //mem_allocator.free(entry, sizeof(LockEntry));
}

void
Row_ww::bring_next() {
  LockEntry * entry;
  // If any waiter can join the owners, just do it!
  while (waiters_head && (owner_cnt == 0 || !conflict_lock(owners->type, waiters_head->type) )) {
    LIST_GET_HEAD(waiters_head, waiters_tail, entry);
    STACK_PUSH(owners, entry);
    entry->status = LOCK_OWNER;
    owner_cnt ++;
    waiter_cnt --;
    ASSERT(entry->txn->lock_ready == 0);
    entry->txn->lock_ready = true;
    lock_type = entry->type;
  }
  ASSERT((owners == NULL) == (owner_cnt == 0));
}
