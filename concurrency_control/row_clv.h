#ifndef ROW_CLV_H
#define ROW_CLV_H


struct CLVLockEntry {
    // type of lock: EX or SH
	lock_t type;
	bool is_cohead;
	bool delta;
	txn_man * txn;
	CLVLockEntry * next;
	CLVLockEntry * prev;
};


class Row_clv {
public:
	//bool is_retire_on;
	void init(row_t * row);
	// [DL_DETECT] txnids are the txn_ids that current txn is waiting for.
    RC lock_get(lock_t type, txn_man * txn);
    RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt);
    RC lock_release(txn_man * txn, RC rc);
    RC lock_retire(txn_man * txn);
    bool has_retired() {
    	return retired_cnt > 1;
    };
	
private:
	#if SPINLOCK
	pthread_spinlock_t * latch;
	#else
    pthread_mutex_t * latch;
    #endif
	bool blatch;
	
	bool 		conflict_lock(lock_t l1, lock_t l2);
	CLVLockEntry * get_entry();
	void 		return_entry(CLVLockEntry * entry);
	row_t * _row;
    UInt32 owner_cnt;
    UInt32 waiter_cnt;
    UInt32 retired_cnt; // no need to keep retied cnt
    ts_t local_ts;
    bool retire_on;
	
	// owners is a single linked list
	// waiters is a double linked list 
	// [waiters] head is the oldest txn, tail is the youngest txn. 
	//   So new txns are inserted into the tail.
	CLVLockEntry * owners;
	CLVLockEntry * owners_tail;
	CLVLockEntry * retired_head;
	CLVLockEntry * retired_tail;
	CLVLockEntry * waiters_head;
	CLVLockEntry * waiters_tail;

	void clean_aborted_retired();
	void clean_aborted_owner();
	CLVLockEntry * rm_if_in_owners(txn_man * txn);
	bool rm_if_in_retired(txn_man * txn, bool is_abort);
	bool rm_if_in_waiters(txn_man * txn);
	CLVLockEntry * rm_from_owners(CLVLockEntry * en, CLVLockEntry * prev, bool destroy=true);
	CLVLockEntry * rm_from_retired(CLVLockEntry * en);
	bool bring_next(txn_man * txn);
	bool has_conflicts_in_list(CLVLockEntry * list, CLVLockEntry * entry);
	bool conflict_lock_entry(CLVLockEntry * l1, CLVLockEntry * l2);
	RC wound_conflict(lock_t type, txn_man * txn, ts_t ts, CLVLockEntry * list, RC status);
	RC wound_txn(txn_man * txn, CLVLockEntry * en);
	void insert_to_waiters(lock_t type, txn_man * txn);
	CLVLockEntry * remove_descendants(CLVLockEntry * en);
	void update_entry(CLVLockEntry * en);

    // debugging method
    void debug();
    void print_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt);
    void assert_notin_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn);
    void assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn);
    void assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, CLVLockEntry * l);

};

#endif
