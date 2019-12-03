#ifndef ROW_CLV_H
#define ROW_CLV_H

#include "row_lock.h"
/*
struct LockEntry {
    // type of lock: EX or SH
	lock_t type;
	txn_man * txn;
	LockEntry * next;
	LockEntry * prev;
};
*/

struct LockEntry;

class Row_clv {
public:
	void init(row_t * row);
	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    RC lock_get(lock_t type, txn_man * txn);
    RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(txn_man * txn);
    RC lock_retire(txn_man * txn);
	
private:
    pthread_mutex_t * latch;
	bool blatch;
	
	bool 		conflict_lock(lock_t l1, lock_t l2);
	LockEntry * get_entry();
	void 		return_entry(LockEntry * entry);
	row_t * _row;
	// owner's lock type
    lock_t lock_type;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;
	
	// owners is a single linked list
	// waiters is a double linked list 
	// [waiters] head is the oldest txn, tail is the youngest txn. 
	//   So new txns are inserted into the tail.
	LockEntry * owners;	
	LockEntry * waiters_head;
	LockEntry * waiters_tail;
	LockEntry * retired;

    void abort_or_dependent(LockEntry * list, txn_man * txn, bool high_first = true);
    void add_dependency(txn_man * high, txn_man * low);
    bool violate(txn_man * high, txn_man * low);
    LockEntry * add_to_owner(lock_t type, txn_man * txn);
    LockEntry * insert_to_waiter(lock_t type, txn_man * txn);
    void add_dependencies(txn_man * high, LockEntry * head);
    bool remove_if_exists(LockEntry * list, txn_man * txn, bool is_owner);
    void bring_next();
};

#endif
