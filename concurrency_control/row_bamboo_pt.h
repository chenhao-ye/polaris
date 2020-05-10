#ifndef ROW_BAMBOO_PT_H
#define ROW_BAMBOO_PT_H

#define UPDATE_RETIRE_INFO(en, prev) { \
    if (prev) { \
      if (conflict_lock(prev->type, en->type)) { \
        en->delta = true; \
        en->txn->increment_commit_barriers(); \
      } else { \
        if (prev->is_cohead) \
          en->is_cohead = true; \
        else \
          en->txn->increment_commit_barriers(); \
     } \
    } else \
      en->is_cohead = true; \
}


#define CHECK_ROLL_BACK(en) { \
  if (!fcw && (en->type == LOCK_EX)) { \
    en->access->orig_row->copy(en->access->orig_data); \
    fcw = en; \
  } \
}

// no need to be too complicated (i.e. call function) as the owner will be empty in the end
#define ABORT_ALL_OWNERS() \
  while(owners) { \
    en = owners; \
    owners = owners->next; \
    en->txn->set_abort(); \
    return_entry(en); \
  } \
  owners_tail = NULL; \
  owners = NULL; \
  owner_cnt = 0;

struct BBLockEntry {
  // type of lock: EX or SH
  lock_t type;
  lock_status status;
  bool is_cohead;
  bool delta;
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
  delta(true), txn(NULL), next(NULL), prev(NULL), access(NULL);
#endif
};

class Row_bamboo_pt {
 public:
  void init(row_t * row);
  // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
  RC lock_get(lock_t type, txn_man * txn, Access * access);
  virtual RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int
  &txncnt, Access * access);
  RC lock_release(void * en, RC rc);
  RC lock_retire(void * en);

 protected:
#if LATCH == LH_SPINLOCK
  pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
  pthread_mutex_t * latch;
#else
  mcslock * latch;
#endif
  bool blatch;

  static bool 		conflict_lock(lock_t l1, lock_t l2);
  BBLockEntry *     get_entry(Access *);
  void 		        return_entry(BBLockEntry * entry);
  void		        lock(BBLockEntry * en);
  void		        unlock(BBLockEntry * en);
  row_t * _row;
  UInt32 owner_cnt;
  UInt32 waiter_cnt;
  UInt32 retired_cnt; // no need to keep retied cnt

  // owners is a single linked list
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

  bool bring_next(txn_man * txn);
  static bool conflict_lock_entry(BBLockEntry * l1, BBLockEntry * l2);
  void update_entry(BBLockEntry * en);
  bool rm_from_retired(BBLockEntry * en, bool is_abort);

 private:
  BBLockEntry * remove_descendants(BBLockEntry * en);
};

#endif
