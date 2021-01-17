#ifndef ROW_LOCK_H
#define ROW_LOCK_H

struct LockEntry {
    txn_man * txn;
    Access * access;
    lock_t type;
    lock_status status;
    LockEntry * next;
    LockEntry * prev;
    LockEntry(txn_man * t, Access * a): txn(t), access(a), type(LOCK_NONE),
                                        status(LOCK_DROPPED), next(NULL), prev(NULL) {};
};

class Row_lock {
  public:
    void init(row_t * row);
    // [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
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
