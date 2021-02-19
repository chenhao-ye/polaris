#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_lock::init(row_t * row) {
  _row = row;
  owners = NULL;
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
void Row_lock::lock(txn_man * txn) {
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
void Row_lock::unlock(txn_man * txn) {
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

RC Row_lock::lock_get(lock_t type, txn_man * txn, Access * access) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt, access);
}

RC Row_lock::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids,
    int &txncnt, Access * access) {
  assert (CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE);
  RC rc;
  int part_id =_row->get_part_id();

  LockEntry * entry = get_entry(access);

#if PF_CS
  uint64_t starttime = get_sys_clock();
#endif
  lock(entry->txn);
  COMPILER_BARRIER
#if PF_CS
  uint64_t endtime = get_sys_clock();
  INC_STATS(txn->get_thd_id(), time_get_latch, endtime - starttime);
  starttime = endtime;
#endif
  assert(owner_cnt <= g_thread_cnt);
  assert(waiter_cnt < g_thread_cnt);
#if DEBUG_ASSERT
  if (owners != NULL)
		assert(lock_type == owners->type); 
	else 
		assert(lock_type == LOCK_NONE);
	LockEntry * en = owners;
	UInt32 cnt = 0;
	while (en) {
		assert(en->txn->get_thd_id() != txn->get_thd_id());
		cnt ++;
		en = en->next;
	}
	assert(cnt == owner_cnt);
	en = waiters_head;
	cnt = 0;
	while (en) {
		cnt ++;
		en = en->next;
	}
	assert(cnt == waiter_cnt);
#endif

  bool conflict = conflict_lock(lock_type, type);
  if (CC_ALG == WAIT_DIE && !conflict) {
    if (waiters_head && txn->get_ts() < waiters_head->txn->get_ts())
      conflict = true;
  }
  // Some txns coming earlier is waiting. Should also wait.
  //if (CC_ALG == DL_DETECT && waiters_head != NULL)
  if (waiters_head != NULL)
    conflict = true;

  if (conflict) {
    // Cannot be added to the owner list.
    if (CC_ALG == NO_WAIT) {
      rc = Abort;
      goto final;
    } else if (CC_ALG == DL_DETECT) {
      //LockEntry * entry = get_entry();
      entry->txn = txn;
      entry->type = type;
      LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
      entry->status = LOCK_WAITER;
      waiter_cnt ++;
      txn->lock_ready = false;
      rc = WAIT;
    } else if (CC_ALG == WAIT_DIE) {
      ///////////////////////////////////////////////////////////
      //  - T is the txn currently running
      //	IF T.ts < ts of all owners
      //		T can wait
      //  ELSE
      //      T should abort
      //////////////////////////////////////////////////////////

      bool canwait = true;
      LockEntry * en = owners;
      while (en != NULL) {
        if (en->txn->get_ts() < txn->get_ts()) {
          canwait = false;
          break;
        }
        en = en->next;
      }
      if (canwait) {
        // insert txn to the right position
        // the waiter list is always in timestamp order
        //LockEntry * entry = get_entry();
        entry->txn = txn;
        entry->type = type;
        en = waiters_head;
        while (en != NULL && txn->get_ts() < en->txn->get_ts())
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
      }
      else {
        // lock abort is not used for wait_die. since abort itself only
        rc = Abort;
        return_entry(entry);
      }
    }
  } else {
    entry->type = type;
    entry->txn = txn;
    txn->lock_ready = true;
    STACK_PUSH(owners, entry);
    entry->status = LOCK_OWNER;
    owner_cnt ++;
    lock_type = type;
    if (CC_ALG == DL_DETECT)
      ASSERT(waiters_head == NULL);
    rc = RCOK;
    //printf("[%p]txn-%lu got %lu\n", entry, txn->get_txn_id(), _row->get_row_id());
  }
  final:

  if (rc == WAIT && CC_ALG == DL_DETECT) {
    // Update the waits-for graph
    ASSERT(waiters_tail->txn == txn);
    txnids = (uint64_t *) mem_allocator.alloc(sizeof(uint64_t) * (owner_cnt + waiter_cnt), part_id);
    txncnt = 0;
    LockEntry * en = waiters_tail->prev;
    while (en != NULL) {
      if (conflict_lock(type, en->type))
        txnids[txncnt++] = en->txn->get_txn_id();
      en = en->prev;
    }
    en = owners;
    if (conflict_lock(type, lock_type))
      while (en != NULL) {
        txnids[txncnt++] = en->txn->get_txn_id();
        en = en->next;
      }
    ASSERT(txncnt > 0);
  }

  COMPILER_BARRIER
  unlock(entry->txn);
#if PF_CS
  INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif

  return rc;
}


RC Row_lock::lock_release(LockEntry * entry) {
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
  LockEntry * en;
  // Try to find the entry in the owners
  if (entry->status == LOCK_OWNER) { // find the entry in the owner list
    en = owners;
    LockEntry * prev = NULL;
    while(en) {
      if (en == entry)
        break;
      prev = en;
      en = en->next;
    }
    // rm from owners
    if (prev)
      prev->next = entry->next;
    else
      owners = entry->next;
    owner_cnt --;
    if (owner_cnt == 0)
      lock_type = LOCK_NONE;
  } else if (entry->status == LOCK_WAITER) {
    // Not in owners list, try waiters list.
    LIST_REMOVE(entry);
    if (entry == waiters_head)
      waiters_head = entry->next;
    if (entry == waiters_tail)
      waiters_tail = entry->prev;
    waiter_cnt --;
  } else {
    assert(false);
  }

  if (owner_cnt == 0)
    ASSERT(lock_type == LOCK_NONE);
#if DEBUG_ASSERT && CC_ALG == WAIT_DIE
  for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
			assert(en->next->txn->get_ts() < en->txn->get_ts());
#endif
  // If any waiter can join the owners, just do it!
  while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
    LIST_GET_HEAD(waiters_head, waiters_tail, en);
    STACK_PUSH(owners, en);
    en->status = LOCK_OWNER;
    owner_cnt ++;
    waiter_cnt --;
    ASSERT(en->txn->lock_ready == false);
    en->txn->lock_ready = true;
    lock_type = en->type;
    //printf("[%p]txn-%lu got %lu\n", en, en->txn->get_txn_id(), _row->get_row_id());
  }
  ASSERT((owners == NULL) == (owner_cnt == 0));
  COMPILER_BARRIER
  unlock(entry->txn);
#if PF_CS
  INC_STATS(entry->txn->get_thd_id(), time_release_cs, get_sys_clock() -
      starttime);
#endif
  return RCOK;
}

bool Row_lock::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)
    return true;
  else
    return false;
}

inline 
LockEntry * Row_lock::get_entry(Access * access) {
  //LockEntry * entry = (LockEntry *) mem_allocator.alloc(sizeof(LockEntry),
  // _row->get_part_id());
  #if CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT
  LockEntry * entry = (LockEntry *) access->lock_entry;
  entry->next = NULL;
  entry->prev = NULL;
  entry->status = LOCK_DROPPED;
  return entry;
  #else 
  return NULL;
  #endif
}

inline 
void Row_lock::return_entry(LockEntry * entry) {
  //mem_allocator.free(entry, sizeof(LockEntry));
  entry->next = NULL;
  entry->prev = NULL;
  entry->type = LOCK_NONE;
  entry->status = LOCK_DROPPED;
}

