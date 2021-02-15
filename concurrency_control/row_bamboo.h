#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

// update cohead info when a newly-init entry (en) is firstly 
// added to owners (bring_next, WR)
// or tail of retired (lock_get/bring_next, RD)
// (not apply when moving from owners to retired
// Algorithm:
//     if previous entry is not null
//         if self is WR,
//             not cohead, need to incr barrier
//         else 
//             if previous entry is RD
//               if prev is cohead, 
//                   //time saved is 0.
//               otherwise, self is not cohead, need to incr barrier,
//                   //record start_ts to calc time saved when becomes cohead
//             else if prev is WR
//               not cohead, need to incr barrier,
//               //record start_ts to calc time saved when becomes cohead
//     else
//         read no dirty data, becomes cohead
//         //record time saved from elr is 0.
//     		
#define UPDATE_RETIRE_INFO(en, prev) { \
  if (prev) { \
    if (en->type == LOCK_EX) \
      en->txn->increment_commit_barriers(); \
    else { \
            if (prev->type == LOCK_SH) { \
              en->is_cohead = prev->is_cohead; \
              if (!en->is_cohead) { \
                en->txn->increment_commit_barriers(); \
              } \
            } else { \
              en->txn->increment_commit_barriers(); } \
    } \
  } else { \
    en->is_cohead = true; \
   } }

// used by lock_retire() (move from owners to retired)
// or by lock_get()/bring_next(), used when has no owners but directly enters retired
// for the latter need to call UPDATE_RETIRE_INFO(to_insert, retired_tail);
#define ADD_TO_RETIRED_TAIL(to_retire) { \
  LIST_PUT_TAIL(retired_head, retired_tail, to_retire); \
  to_retire->status = LOCK_RETIRED; \
  retired_cnt++; }

// Insert to_insert(RD) into the tail when owners is not empty
// (1) update inserted entry's cohead information
// (2) NEED to update owners cohead information 
//     if owner is not cohead, it cannot become one with RD inserted
//     if owner is cohead, RD becomes cohead and owner is no longer a cohead 
#define INSERT_TO_RETIRED_TAIL(to_insert) { \
  UPDATE_RETIRE_INFO(to_insert, retired_tail); \
  if (owners && owners->is_cohead) { \
    owners->is_cohead = false; \
    owners->txn->increment_commit_barriers(); \
  } \
  LIST_PUT_TAIL(retired_head, retired_tail, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; }

#define RETIRE_ENTRY(to_retire) { \
  to_retire = owners; \
  owners = NULL; \
  to_retire->next=NULL; \
  to_retire->prev=NULL; \
  ADD_TO_RETIRED_TAIL(to_retire); }

#define CHECK_ROLL_BACK(en) { \
    en->access->orig_row->copy(en->access->orig_data); \
}

#define DEC_BARRIER_PF(entry) { \
    assert(!entry->is_cohead); \
    entry->is_cohead = true; \
    uint64_t starttime = get_sys_clock(); \
    entry->txn->decrement_commit_barriers(); \
    INC_STATS(entry->txn->get_thd_id(), time_semaphore_cs, \
        get_sys_clock() - starttime); \
}

#define DEC_BARRIER(entry) { \
    assert(!entry->is_cohead); \
    entry->is_cohead = true; \
    entry->txn->decrement_commit_barriers(); \
}

struct BBLockEntry {
    // type of lock: EX or SH
    txn_man * txn;
    Access * access;
    //uint8_t padding[64 - sizeof(void *)*2];
    lock_t type;
    uint8_t padding[64 - sizeof(void *)*2 - sizeof(lock_t)];
    BBLockEntry * next;
    //uint8_t padding[64 - sizeof(void *)*3 - sizeof(lock_t)];
    bool is_cohead;
    lock_status status;
    BBLockEntry * prev;
    BBLockEntry(txn_man * t, Access * a): txn(t), access(a), type(LOCK_NONE),
                                          next(NULL), is_cohead(false),
                                          status(LOCK_DROPPED),
                                          prev(NULL) {};
};

class Row_bamboo {
  public:
    void init(row_t * row);
    RC lock_get(lock_t type, txn_man * txn, Access * access);
    RC lock_release(BBLockEntry * entry, RC rc);
    RC lock_retire(BBLockEntry * entry);

  private:
    // data structure
    BBLockEntry * owners;
    BBLockEntry * retired_head;
    BBLockEntry * retired_tail;
    BBLockEntry * waiters_head;
    BBLockEntry * waiters_tail;
    row_t * _row;
    UInt32 waiter_cnt;
    UInt32 retired_cnt;
    // latches
#if LATCH == LH_SPINLOCK
    pthread_spinlock_t * latch;
#elif LATCH == LH_MUTEX
    pthread_mutex_t * latch;
#else
    mcslock * latch;
#endif
    bool blatch;

    // helper functions
    bool              bring_next(txn_man * txn);
    void              update_entry(BBLockEntry * en);
    BBLockEntry *     rm_from_retired(BBLockEntry * en, bool is_abort, txn_man * txn);
    BBLockEntry *     remove_descendants(BBLockEntry * en, txn_man * txn);
    void              lock(txn_man * txn);
    void              unlock(txn_man * txn);
	RC                insert_read_to_retired(BBLockEntry * to_insert, ts_t ts, Access * access);
#if DEBUG_BAMBOO
	void              check_correctness();
#endif

    // check priorities
    inline static bool a_higher_than_b(ts_t a, ts_t b) {
        return a < b;
    };

    inline static int assign_ts(ts_t ts, txn_man * txn) {
        if (ts == 0) {
            ts = txn->set_next_ts(1);
            // if fail to assign, reload
            if ( ts == 0 )
                ts = txn->get_ts();
        }
        return ts;
    };


    // init a lock entry (pre-allocated in each txn's access)
    static BBLockEntry * get_entry(Access * access) {
    #if CC_ALG == BAMBOO
        BBLockEntry * entry = access->lock_entry;
        entry->txn->lock_ready = false;
        // dont init lock_abort, can only be set true but not false. 
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
        entry->is_cohead = false;
        return entry;
    #else
        return NULL;
    #endif
    };


    // clean the lock entry
    void return_entry(BBLockEntry * entry) {
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
    };

	inline bool bring_out_waiter(BBLockEntry * entry, txn_man * txn) {
		LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
		entry->txn->lock_ready = true;
		if (txn == entry->txn) {
			return true;
		}
		return false;
	};

	inline void add_to_waiters(ts_t ts, BBLockEntry * to_insert) {
		BBLockEntry * en = waiters_head;
		while (en != NULL) {
			if (ts < en->txn->get_ts())
					break;
			en = en->next;
		}
		if (en) {
			LIST_INSERT_BEFORE(en, to_insert);
			if (en == waiters_head) 
				waiters_head = to_insert;
		} else {
			LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
		}
		to_insert->status = LOCK_WAITER;
		to_insert->txn->lock_ready = false;
		waiter_cnt++;
        assert(ts != 0);
	};	

	// NOTE: it is unrealistic to have completely ordered read with
	// dynamically assigned ts. e.g. [0,0,0] -> [12, 11, 5]
	// used when to_insert->type = LOCK_SH
	inline RC wound_retired_rd(ts_t ts, BBLockEntry * to_insert) {
		BBLockEntry * en = retired_head;
		while(en) {
			if (en->type == LOCK_EX && a_higher_than_b(ts, en->txn->get_ts())) {
				if (to_insert->txn->wound_txn(en->txn) == COMMITED) {
					//return_entry(to_insert);
					//return Abort;
                    en = en->next;
                    continue;
				}
				en = rm_from_retired(en, true, to_insert->txn);
			} else 
				en = en->next;
		}
		return RCOK;
	};

	// try_wound(to_wound, wounder), if commited, wound failed, return wounder
	inline RC wound_retired_wr(ts_t ts, BBLockEntry * to_insert) {
		BBLockEntry * en = retired_head;
		while(en) {
			if (en->txn->get_ts() == 0 || a_higher_than_b(ts, en->txn->get_ts())) {
				if (to_insert->txn->wound_txn(en->txn) == COMMITED) {
					//return_entry(to_insert);
					//return Abort;
                    en = en->next;
                    continue;
				}
				en = rm_from_retired(en, true, to_insert->txn);
			} else 
				en = en->next;
		}
		return RCOK;
	};

	inline RC wound_owner(BBLockEntry * to_insert) {
		if (to_insert->txn->wound_txn(owners->txn) == COMMITED) {
			//return_entry(to_insert);
			//return Abort;
            return WAIT;
		}
		return_entry(owners);
		owners = NULL;
		return RCOK;
	};

// owner's type is always LOCK_EX
#define WOUND_OWNER(to_insert) { \
    TRY_WOUND(owners, to_insert); \
    return_entry(owners); \
    owners = NULL; \
}


};
#endif
