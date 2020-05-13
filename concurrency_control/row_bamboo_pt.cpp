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

  // allocate lock entry
  BBLockEntry * to_insert = get_entry(access);
  to_insert->type = type;
  // helper
  BBLockEntry * en;
  BBLockEntry * to_return = NULL;

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(to_insert);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
  starttime = get_sys_clock();
#endif

  // each thread has at most one owner of a lock
  assert(owner_cnt <= g_thread_cnt);
  // each thread has at most one waiter
  assert(waiter_cnt < g_thread_cnt);
  assert(retired_cnt <= g_thread_cnt);

  // assign a ts if ts == 0
  if (txn->get_ts() == 0)
    txn->set_next_ts(1);

  // 1. set txn to abort in owners and retired
  RC rc = RCOK;
  ts_t ts = txn->get_ts();

  // check if can grab directly
  if (retired_cnt == 0 && owner_cnt == 0 && waiter_cnt == 0) {
    LIST_PUT_TAIL(owners, owners_tail, to_insert);
    to_insert->status = LOCK_OWNER;
    owner_cnt++;
    txn->lock_ready = true;
    unlock(to_insert);
    return rc;
  }

  fcw = NULL;
  // check retired and wound conflicted
  en = retired_head;
  while (en != NULL) {
#if !RETIRE_ON
printf("en is %p\n", (void *)en);
ASSERT(false);
#endif
    // self assigned, if conflicted, assign a number
    if (rc == RCOK && conflict_lock(en->type, type) && (en->txn->get_ts() > ts))
      rc = WAIT;
    if (rc == WAIT && (en->txn->get_ts() > ts)) {
      TRY_WOUND_PT(en, to_insert);
      en = rm_from_retired(en, true);
    } else {
      en = en->next;
    }
  }

  // check owners
  en = owners;
  while (en != NULL) {
    if (rc == RCOK && conflict_lock(en->type, type) && (en->txn->get_ts() > ts))
      rc = WAIT;
    if (rc == WAIT && (en->txn->get_ts() > ts)) {
      TRY_WOUND_PT(en, to_insert);
      LIST_RM(owners, owners_tail, en, owner_cnt);
      to_return = en;
      en = en->next;
      return_entry(to_return);
    } else {
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
    LIST_INSERT_BEFORE(en, to_insert);
    if (en == waiters_head)
      waiters_head = to_insert;
  } else {
    LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
  }
  to_insert->status = LOCK_WAITER;
  waiter_cnt ++;

  // 3. if brought txn in owner, return acquired lock
  if (bring_next(txn)) {
    rc = RCOK;
  } else {
    txn->lock_ready = false; // wait in waiters
    rc = WAIT;
  }

#if RETIRE_ON
  // 5. move reads to retired if RETIRE_READ=false
  if (owners && (waiter_cnt > 0) && (owners->type == LOCK_SH)) {
    // if retire turned on and share lock is the owner
    // move to retired
    BBLockEntry * to_retire = NULL;
    while (owners) {
      to_retire = owners;
      RETIRE_ENTRY(to_retire);
    }
    if (bring_next(txn)) {
      rc = RCOK;
    }
  }
#endif
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - starttime);
#endif
  unlock(to_insert);
  return rc;
}

RC Row_bamboo_pt::lock_retire(void * addr) {
  BBLockEntry * entry = (BBLockEntry *) addr;
//assert(entry->type == LOCK_EX);
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
    RETIRE_ENTRY(entry);
    // make dirty data globally visible
    if (entry->type == LOCK_EX)
      entry->access->orig_row->copy(entry->access->data);
  } else {
    // may be is aborted: assert(txn->status == ABORTED);
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
    fcw = NULL;
printf("rm txn %lu from row %p 's retired\n", entry->txn->get_txn_id(), (void *)_row);
    rm_from_retired(entry, rc == Abort);
  } else if (entry->status == LOCK_OWNER) {
printf("rm txn %lu from row %p 's owners\n", entry->txn->get_txn_id(), (void *)_row);
    LIST_RM(owners, owners_tail, entry, owner_cnt);
ASSERT((owners == NULL) == (owner_cnt == 0));
    // not found in retired, need to make globally visible if rc = commit
    if (rc == RCOK && (entry->type == LOCK_EX))
       entry->access->orig_row->copy(entry->access->data);
  } else if (entry->status == LOCK_WAITER) {
printf("rm txn %lu from row %p 's waiters\n", entry->txn->get_txn_id(), (void *)_row);
    LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
  } else {
printf("rm txn %lu from row %p 's not found\n", entry->txn->get_txn_id(), (void *)_row);
  }
  return_entry(entry);
  if (owner_cnt == 0) {
ASSERT((owners == NULL) == (owner_cnt == 0));
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

/*
 * return next lock entry in the retired list after removing en (and its
 * descendants if is_abort = true)
 */
inline 
BBLockEntry * Row_bamboo_pt::rm_from_retired(BBLockEntry * en, bool is_abort) {
  if (is_abort) {
    CHECK_ROLL_BACK(en); // roll back only for the first-conflicting-write
  }
  if (is_abort && (en->type == LOCK_EX)) {
    en->txn->lock_abort = true;
    en = remove_descendants(en, en->txn);
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
  ASSERT((owners == NULL) == (owner_cnt == 0));
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
  BBLockEntry * entry = (BBLockEntry *) access->lock_entry;
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
      if (entry->prev->type == LOCK_EX)
        entry->next->delta = true;
      else
        entry->next->delta = false;
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
    en = en->next;
    return_entry(to_return);
  }
  // empty owners
  ABORT_ALL_OWNERS(en);
  assert(!retired_head || retired_head->is_cohead);

  if (prev)
    return prev->next;
  else
    return retired_head;
}
