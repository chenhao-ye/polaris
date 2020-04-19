#include "row.h"
#include "txn.h"
#include "row_bamboo_pt.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_bamboo_pt::init(row_t * row) {
  _row = row;
  // owners is a single linked list, each entry/node contains info like lock type, prev/next
  owners = NULL;
  owners_tail = NULL;
  // waiter is a double linked list. two ptrs to the linked lists
  waiters_head = NULL;
  waiters_tail = NULL;
  // retired is a linked list, the next of tail is the head of owners
  retired_head = NULL;
  retired_tail = NULL;
  owner_cnt = 0;
  waiter_cnt = 0;
  retired_cnt = 0;

#if SPINLOCK
  latch = new pthread_spinlock_t;
  pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#else
  latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#endif
  blatch = false;
}

inline void Row_bamboo_pt::lock() {
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

inline void Row_bamboo_pt::unlock() {
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

RC Row_bamboo_pt::lock_get(lock_t type, txn_man * txn) {
  uint64_t *txnids = NULL;
  int txncnt = 0;
  return lock_get(type, txn, txnids, txncnt);
}


RC Row_bamboo_pt::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
  assert (CC_ALG == BAMBOO);
  // move malloc out
  BBLockEntry * entry = get_entry();
  entry->txn = txn;
  entry->type = type;
  // a linked list storing entries to return in the end

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock();
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
    owner_cnt++;
    txn->lock_ready = true;
    unlock();
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
        unlock();
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
        unlock();
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
      RETIRED_LIST_PUT_TAIL(retired_head, retired_tail, to_retire);
      retired_cnt++;
    }
    if (bring_next(txn)) {
      rc = RCOK;
    }
  }
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - starttime);
#endif

  unlock();
  return rc;
}

RC Row_bamboo_pt::lock_retire(txn_man * txn) {

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
  BBLockEntry * prev = NULL;
  while (entry) {
    if (entry->txn == txn)
      break;
    prev = entry;
    entry = entry->next;
  }
  if (entry) {
    // rm from owners
    QUEUE_RM(owners, owners_tail, prev, entry, owner_cnt);
    entry->next=NULL;
    entry->prev=NULL;
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
    RETIRED_LIST_PUT_TAIL(retired_head, retired_tail, entry);
    retired_cnt++;
  } else {
    // may be is aborted
    assert(txn->status == ABORTED);
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

RC Row_bamboo_pt::lock_release(txn_man * txn, RC rc) {
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
    BBLockEntry * prev = NULL;
    while (en) {
      if (en->txn == txn)
        break;
      prev = en;
      en = en->next;
    }
    if (en) {
      // found in owner, rm it
      QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
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
  }
  if (owner_cnt == 0)
    bring_next(NULL);

#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), debug7, get_sys_clock() - starttime);
#endif
  unlock();
  if (en)
    return_entry(en);
  return RCOK;
}

bool
Row_bamboo_pt::rm_if_in_retired(txn_man * txn, bool is_abort) {
  BBLockEntry * en = retired_head;
  while(en) {
    if (en->txn == txn) {
      if (is_abort) {
        en = remove_descendants(en);
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

bool
Row_bamboo_pt::bring_next(txn_man * txn) {
  bool has_txn = false;
  BBLockEntry * entry;
  // If any waiter can join the owners, just do it!
  while (waiters_head) {
    if ((owner_cnt == 0) || (!conflict_lock(owners->type, waiters_head->type))) {
      LIST_GET_HEAD(waiters_head, waiters_tail, entry);
      waiter_cnt --;
      // add to onwers
      QUEUE_PUSH(owners, owners_tail, entry);
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


bool Row_bamboo_pt::conflict_lock(lock_t l1, lock_t l2) {
  if (l1 == LOCK_NONE || l2 == LOCK_NONE)
    return false;
  else if (l1 == LOCK_EX || l2 == LOCK_EX)
    return true;
  else
    return false;
}

bool Row_bamboo_pt::conflict_lock_entry(BBLockEntry * l1, BBLockEntry * l2) {
  if (l1 == NULL || l2 == NULL)
    return false;
  return conflict_lock(l1->type, l2->type);
}


BBLockEntry * Row_bamboo_pt::get_entry() {
  BBLockEntry * entry = (BBLockEntry *) mem_allocator.alloc(sizeof(BBLockEntry), _row->get_part_id());
  entry->prev = NULL;
  entry->next = NULL;
  entry->delta = false;
  entry->is_cohead = false;
  entry->txn = NULL;
  return entry;
}

void Row_bamboo_pt::return_entry(BBLockEntry * entry) {
  mem_allocator.free(entry, sizeof(BBLockEntry));
}

BBLockEntry *
Row_bamboo_pt::remove_descendants(BBLockEntry * en) {
  BBLockEntry * next;
  assert(en != NULL);
  BBLockEntry * prev = en->prev;
  // 1. remove self, set iterator to next entry
  lock_t type = en->type;
  bool conflict_with_owners = conflict_lock_entry(en, owners);
  next = en->next;
  update_entry(en);
  LIST_RM(retired_head, retired_tail, en, retired_cnt);
  return_entry(en);
  if (en->type == LOCK_SH) {
    if (prev)
      return prev->next;
    else
      return retired_head;
  }
  en = next;
  // 2. remove next conflict till end
  // 2.1 find next conflict
  while(en && (!conflict_lock(type, en->type))) {
    en = en->next;
  }
  // 2.2 remove dependees
  if (en == NULL) {
    if (!conflict_with_owners) {
      // clean owners
      while(owners) {
        en = owners;
        en->txn->set_abort();
        // no need to be too complicated (i.e. call function) as the owner will be empty in the end
        owners = owners->next;
        return_entry(en);
      }
      owners_tail = NULL;
      owners = NULL;
      owner_cnt = 0;
    } // else, nothing to do
  } else {
    // abort till end
    LIST_RM_SINCE(retired_head, retired_tail, en);
    while(en) {
      next = en->next;
      en->txn->set_abort();
      retired_cnt--;
      return_entry(en);
      en = next;
    }
  }
  if (prev)
    return prev->next;
  else
    return retired_head;
}


void
Row_bamboo_pt::update_entry(BBLockEntry * en) {
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
