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
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;
	
	// owners is a single linked list
	// waiters is a double linked list 
	// [waiters] head is the oldest txn, tail is the youngest txn. 
	//   So new txns are inserted into the tail.
	LockEntry * owners;
	LockEntry * owners_tail;
	LockEntry * retired;
	LockEntry * retired_tail;
	LockEntry * waiters_head;
	LockEntry * waiters_tail;

	void bring_next();
	RC check_abort(lock_t type, txn_man * txn, LockEntry * list, bool is_owner);
	void insert_to_waiters(lock_t type, txn_man * txn);
    LockEntry * remove_if_exists(LockEntry * list, txn_man * txn, bool is_owner);

};

#endif
