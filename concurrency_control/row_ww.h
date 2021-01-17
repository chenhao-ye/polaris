#ifndef ROW_WW_H
#define ROW_WW_H

#include "row_lock.h"

class Row_ww {
 public:
  void init(row_t * row);
  RC lock_get(lock_t type, txn_man * txn, Access * access);
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt, Access * access);
  RC lock_release(LockEntry * entry);
  void lock(txn_man * txn);
  void unlock(txn_man * txn);

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
  void        bring_next();

  row_t * _row;
  // owner's lock type
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
