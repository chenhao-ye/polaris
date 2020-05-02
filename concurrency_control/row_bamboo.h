#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

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
  bool is_cohead;
  bool delta;
  txn_man * txn;
  BBLockEntry * next;
  BBLockEntry * prev;
  Access * access;
};

class Row_bamboo {
 public:
  void init(row_t * row);
  // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
  RC lock_get(lock_t type, txn_man * txn, Access * access);
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt,
      Access * access);
  RC lock_release(txn_man * txn, RC rc);
  RC lock_retire(txn_man * txn);

 private:
#if SPINLOCK
  pthread_spinlock_t * latch;
#else
  pthread_mutex_t * latch;
#endif
  bool blatch;

  static bool 		conflict_lock(lock_t l1, lock_t l2);
  BBLockEntry * get_entry();
  void 		return_entry(BBLockEntry * entry);
  void		lock();
  void		unlock();
  row_t * _row;
  UInt32 owner_cnt;
  UInt32 waiter_cnt;
  UInt32 retired_cnt; // no need to keep retied cnt
  ts_t local_ts;
  ts_t txn_ts;

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
  bool rm_if_in_retired(txn_man * txn, bool is_abort);
  RC wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status);
  bool wound_txn(BBLockEntry* en, txn_man* txn, bool check_retired);
  BBLockEntry * remove_descendants(BBLockEntry * en, txn_man * txn);
};

#endif
