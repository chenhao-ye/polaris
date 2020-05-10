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

inline ALWAYS_INLINE
RC Row_ww::lock(LockEntry * en) {
  // take latch
  if (g_central_man)
    glob_manager->lock_row(_row);
  else
  {
#if LATCH == LH_SPINLOCK
    pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
    pthread_mutex_lock( latch );
#else
    latch->acquire(en->m_node);
#endif
  }
}

inline ALWAYS_INLINE
RC Row_ww::unlock(LockEntry * en) {
  // release latch
  if (g_central_man)
    glob_manager->release_row(_row);
  else
  {
#if LATCH == LH_SPINLOCK
    pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
    pthread_mutex_unlock( latch );
#else
    latch->release(en->m_node);
#endif
  }
}

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
  LockEntry * to_return = NULL;
#if DEBUG_CS_PROFILING
  uint32_t abort_cnt = 0;
  uint32_t abort_try = 0;
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
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
#if DEBUG_CS_PROFILING
        bool already_aborted = (en->txn->status == ABORTED);
#endif
        if (txn->wound_txn(en->txn) == COMMITED){
          // curr txn is wounded by other txns or txn to wound comitted
          // already.. either way no entry is removed
          if (owner_cnt == 0)
            bring_next();
          rc = Abort;
          return_entry(entry);
          goto final;
        }
#if DEBUG_CS_PROFILING
        abort_try++;
        if (!already_aborted)
          abort_cnt++;
#endif
        // remove from owner
        if (prev)
          prev->next = en->next;
        else {
          if (owners == en)
            owners = en->next;
        }
        // free en
        to_return = en;
        // update count
        owner_cnt--;
        if (owner_cnt == 0)
          lock_type = LOCK_NONE;
      } else {
        prev = en;
      }
      en = en->next;
      if (to_return) {
        return_entry(to_return);
        to_return = NULL;
      }
    }
#if DEBUG_CS_PROFILING
    // max abort chain
    if (abort_cnt > stats._stats[txn->get_thd_id()]->debug10)
      stats._stats[txn->get_thd_id()]->debug10 = abort_try;
    // max length of aborts
    if (abort_cnt > stats._stats[txn->get_thd_id()]->debug11)
      stats._stats[txn->get_thd_id()]->debug11 = abort_cnt;
#endif

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
  unlock(entry);
  return rc;
}


RC Row_ww::lock_release(void * addr) {

  auto entry = (LockEntry * ) addr;

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug6, get_sys_clock() - starttime);
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
    return_entry(entry);
  } else if (entry->status == LOCK_WAITER) {
    LIST_REMOVE(entry);
    if (entry == waiters_head)
      waiters_head = entry->next;
    if (entry == waiters_tail)
      waiters_tail = entry->prev;
    waiter_cnt --;
    return_entry(entry);
  }
  bring_next();
  ASSERT((owners == NULL) == (owner_cnt == 0));
  unlock(entry);
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

inline ALWAYS_INLINE
LockEntry * Row_ww::get_entry(Access * access) {
  //LockEntry * entry = (LockEntry *) mem_allocator.alloc(sizeof(LockEntry),
  // _row->get_part_id());
  return (LockEntry *) access->lock_entry;
}

inline ALWAYS_INLINE
void Row_ww::return_entry(LockEntry * entry) {
  //mem_allocator.free(entry, sizeof(LockEntry));
  entry->next = NULL;
  entry->prev = NULL;
  entry->type = LOCK_NONE;
  entry->status = LOCK_DROPPED;
}

void
Row_ww::bring_next() {
  LockEntry * entry;
  // If any waiter can join the owners, just do it!
  while (waiters_head && (owner_cnt == 0 || !conflict_lock(owners->type, waiters_head->type) )) {
    LIST_GET_HEAD(waiters_head, waiters_tail, entry);
    STACK_PUSH(owners, entry);
    owner_cnt ++;
    waiter_cnt --;
    ASSERT(entry->txn->lock_ready == 0);
    entry->txn->lock_ready = true;
    lock_type = entry->type;
  }
  ASSERT((owners == NULL) == (owner_cnt == 0));
}
