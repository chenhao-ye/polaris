#ifndef ROW_CLVP_H
#define ROW_CLVP_H

//#include "row_clv.h"

struct CLVLockEntry {
    // type of lock: EX or SH
	lock_t type;
	bool is_cohead;
	bool delta;
	txn_man * txn;
	CLVLockEntry * next;
	CLVLockEntry * prev;
};


class Row_clvp {
public:
	void init(row_t * row);
	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    RC lock_get(lock_t type, txn_man * txn);
    RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(txn_man * txn, RC rc);
    RC lock_retire(txn_man * txn);
	
private:
    pthread_mutex_t * latch;
	bool blatch;
	
	bool 		conflict_lock(lock_t l1, lock_t l2);
	CLVLockEntry * get_entry();
	void 		return_entry(CLVLockEntry * entry);
	row_t * _row;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    UInt32 retired_cnt; // no need to keep retied cnt
	
	// owners is a single linked list
	// waiters is a double linked list 
	// [waiters] head is the oldest txn, tail is the youngest txn. 
	//   So new txns are inserted into the tail.
	CLVLockEntry * owners;
	CLVLockEntry * owners_tail;
	CLVLockEntry * retired;
	CLVLockEntry * retired_tail;
	CLVLockEntry * waiters_head;
	CLVLockEntry * waiters_tail;

	void bring_next();
	void insert_to_waiters(lock_t type, txn_man * txn);
	CLVLockEntry * remove_if_exists(CLVLockEntry * list, txn_man * txn, bool is_owner, bool is_abort);
	RC check_abort(lock_t type, txn_man * txn, CLVLockEntry * list, bool is_owner);
	bool has_conflicts_in_list(CLVLockEntry * list, CLVLockEntry * entry);
	bool conflict_lock_entry(CLVLockEntry * l1, CLVLockEntry * l2);
	void update_entry(CLVLockEntry * en);
	RC remove_descendants(CLVLockEntry * en, txn_man * txn, lock_t type);
    
    // debugging method
    void print_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt);
    void assert_notin_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn);
    void assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn);

};

#endif
