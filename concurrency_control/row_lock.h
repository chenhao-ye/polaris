#ifndef ROW_LOCK_H
#define ROW_LOCK_H

struct LockEntry {
  lock_t type;
  lock_status status;
  txn_man * txn;
  LockEntry * next;
  LockEntry * prev;
  Access * access;
#if LATCH == LH_MCSLOCK
  mcslock::qnode_t * m_node;
  LockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), txn(NULL), next(NULL),
  prev(NULL), access(NULL), m_node(NULL) {};
#else
  LockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), txn(NULL), next(NULL),
  prev(NULL), access(NULL) {};
#endif
};

class Row_lock {
 public:
  void init(row_t * row);
  // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
  RC lock_get(lock_t type, txn_man * txn, Access * access);
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt, Access * access);
  RC lock_release(void * en);
  RC lock(LockEntry * en = NULL);
  RC unlock(LockEntry * en = NULL);

 private:
#if LATCH == LH_SPINLOCK
  pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
  pthread_mutex_t * latch;
#else
  mcslock * latch;
#endif
  bool blatch;

  bool 		conflict_lock(lock_t l1, lock_t l2);
  static LockEntry * get_entry(Access * access);
  static void 		return_entry(LockEntry * entry);
  row_t * _row;
  lock_t lock_type;
  UInt32 owner_cnt;
  UInt32 waiter_cnt;

  // owners is a single linked list
  // waiters is a double linked list
  // [waiters] head is the oldest txn, tail is the youngest txn.
  //   So new txns are inserted into the tail.
  LockEntry * owners;
  LockEntry * waiters_head;
  LockEntry * waiters_tail;
};

#endif
