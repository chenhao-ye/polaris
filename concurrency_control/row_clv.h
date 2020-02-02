#ifndef ROW_CLV_H
#define ROW_CLV_H


struct CLVLockEntry {
	// type of lock: EX or SH
	lock_t type;
	bool is_cohead;
	bool delta;
	bool wounded;
	txn_man * txn;
	CLVLockEntry * next;
	CLVLockEntry * prev;
	#if DEBUG_TMP
	loc_t loc;
	#endif
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
	// bool has_retired() {
	// 	return retired_cnt > (g_thread_cnt);
	// };
	
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
	void		lock();
	void		unlock();
	row_t * _row;
	UInt32 owner_cnt;
	UInt32 waiter_cnt;
	UInt32 retired_cnt; // no need to keep retied cnt
	ts_t local_ts;
	bool retire_on;

	#if DEBUG_TMP	
	CLVLockEntry ** vec;
	#endif
	void reset_entry(CLVLockEntry * entry);
	
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

	
	bool bring_next(txn_man * txn);
	bool conflict_lock_entry(CLVLockEntry * l1, CLVLockEntry * l2);
	void insert_to_waiters(CLVLockEntry * entry, lock_t type, txn_man * txn);
	void update_entry(CLVLockEntry * en);
	void batch_return(CLVLockEntry * to_return);
	void batch_wound(CLVLockEntry * to_return);
	#if BATCH_RETURN_ENTRY
	bool rm_if_in_retired(txn_man * txn, bool is_abort, CLVLockEntry *& to_return);
	RC wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status, CLVLockEntry *& to_return);
	bool wound_txn(CLVLockEntry* en, txn_man* txn, bool check_retired, CLVLockEntry *& to_return);
	CLVLockEntry * remove_descendants(CLVLockEntry * en, CLVLockEntry *& to_return);
	#else
	bool rm_if_in_retired(txn_man * txn, bool is_abort);
	RC wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status);
	bool wound_txn(CLVLockEntry* en, txn_man* txn, bool check_retired);
	CLVLockEntry * remove_descendants(CLVLockEntry * en);
	#endif
};

#endif
