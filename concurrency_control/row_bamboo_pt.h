#ifndef ROW_BAMBOO_PT_H
#define ROW_BAMBOO_PT_H

// note: RW (Write-After-Read does not form commit dependency)
#define RECHECK_RETIRE_INFO(en, prev) { \
  bool is_cohead = en->is_cohead; \
  if (prev) { \
    if (prev->type == LOCK_SH) { \
      en->delta = false;  \
      en->is_cohead = prev->is_cohead; \
      if (!en->is_cohead && is_cohead) \
        en->txn->increment_commit_barriers(); \
      if (en->is_cohead && !is_cohead) \
        en->txn->decrement_commit_barriers(); \
    } else { \
      en->delta = true; \
      en->is_cohead = false; \
      if (is_cohead) \
        en->txn->increment_commit_barriers(); } \
  } else { \
    en->is_cohead = true; \
    en->delta = false; \
    if (!is_cohead) \
      en->txn->decrement_commit_barriers(); } \
}

#define UPDATE_RETIRE_INFO(en, prev) { \
  if (prev) { \
    if (prev->type == LOCK_SH) { \
      en->delta = false;  \
      en->is_cohead = prev->is_cohead; \
      if (!en->is_cohead) \
        en->txn->increment_commit_barriers(); \
    } else { \
      en->delta = true; \
      en->is_cohead = false; \
      en->txn->increment_commit_barriers(); } \
  } else { \
    en->is_cohead = true; \
    en->delta = false; } }

#define ADD_TO_RETIRED_TAIL(to_retire) { \
  UPDATE_RETIRE_INFO(to_retire, retired_tail); \
  LIST_PUT_TAIL(retired_head, retired_tail, to_retire); \
  to_retire->status = LOCK_RETIRED; \
  retired_cnt++; }

#define INSERT_TO_RETIRED(to_insert, en) { \
  UPDATE_RETIRE_INFO(to_insert, en->prev); \
  RECHECK_RETIRE_INFO(en, to_insert); \
  LIST_INSERT_BEFORE_CH(retired_head, en, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; \
}

#define ADD_TO_WAITERS(en, to_insert) { \
  rc = WAIT; \
  en = waiters_head; \
  while (en != NULL) { \
    if (ts < en->txn->get_ts()) \
      break; \
    en = en->next; \
  } \
  if (en) { \
    LIST_INSERT_BEFORE(en, to_insert); \
    if (en == waiters_head) \
      waiters_head = to_insert; \
  } else { \
    LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert); \
  } \
  to_insert->status = LOCK_WAITER; \
  waiter_cnt ++; \
}

#define ADD_TO_WAITERS_TAIL(to_insert) { \
  rc = WAIT; \
  LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert); \
  to_insert->status = LOCK_WAITER; \
  waiter_cnt ++; \
}

#define RETIRE_ENTRY(to_retire) { \
  to_retire = owners; \
  owners = NULL; \
  to_retire->next=NULL; \
  to_retire->prev=NULL; \
  ADD_TO_RETIRED_TAIL(to_retire); }

#define CHECK_ROLL_BACK(en) { \
    en->access->orig_row->copy(en->access->orig_data); \
  }

// try_wound(to_wound, wounder), if commited, wound failed, return wounder
#define TRY_WOUND_PT(to_wound, wounder) { \
  if (wounder->txn->wound_txn(to_wound->txn) == COMMITED) {\
    return_entry(wounder); \
    rc = Abort; \
    goto final; } \
}

struct BBLockEntry {
  // type of lock: EX or SH
  lock_t type;
  lock_status status;
  bool is_cohead;
  bool delta; // if conflict with prev
  txn_man * txn;
  BBLockEntry * next;
  BBLockEntry * prev;
  Access * access;
#if LATCH == LH_MCSLOCK
  mcslock::qnode_t * m_node;
  BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
                 delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL), m_node(NULL){};
#else
  BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
  delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL) {};
#endif
};

class Row_bamboo_pt {
 public:
  virtual void init(row_t * row);
  // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
  virtual RC lock_get(lock_t type, txn_man * txn, Access * access);
  virtual RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int
  &txncnt, Access * access);
  virtual RC lock_release(void * en, RC rc);
  virtual RC lock_retire(void * en);

 protected:
#if LATCH == LH_SPINLOCK
  pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
  pthread_mutex_t * latch;
#else
  mcslock * latch;
#endif
  bool blatch;

  virtual bool 		        conflict_lock(lock_t l1, lock_t l2);
  virtual bool              conflict_lock_entry(BBLockEntry * l1,BBLockEntry * l2);
  virtual BBLockEntry *     get_entry(Access *);
  virtual void 		        return_entry(BBLockEntry * entry);
  virtual void		        lock(BBLockEntry * en);
  virtual void		        unlock(BBLockEntry * en);
  virtual bool              bring_next(txn_man * txn);
  virtual void              update_entry(BBLockEntry * en);
  virtual BBLockEntry *     rm_from_retired(BBLockEntry * en, bool is_abort);
  virtual BBLockEntry *     remove_descendants(BBLockEntry * en, txn_man * txn);
  row_t * _row;
  UInt32 owner_cnt;
  UInt32 waiter_cnt;
  UInt32 retired_cnt; // no need to keep retied cnt

  // owners is a double linked list
  // waiters is a double linked list
  // [waiters] head is the oldest txn, tail is the youngest txn.
  //   So new txns are inserted into the tail.
  BBLockEntry * owners;
  BBLockEntry * owners_tail;
  BBLockEntry * retired_head;
  BBLockEntry * retired_tail;
  BBLockEntry * waiters_head;
  BBLockEntry * waiters_tail;
  BBLockEntry * fcw;
};

#endif
