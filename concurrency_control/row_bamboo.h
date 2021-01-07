#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

// update cohead info when a newly-init entry (en) is firstly 
// added to owners (bring_next, WR)
// or tail of retired (lock_get/bring_next, RD)
// (not apply when moving from owners to retired
// Algorithm:
//     if previous entry is not null
//         if previous entry is RD
//             if prev is cohead, then self is cohead, 
//                 time saved is 0.
//             otherwise, self is not cohead, need to incr barrier,
//                 record start_ts to calc time saved when becomes cohead
//         else if prev is WR
//             not cohead, need to incr barrier,
//             record start_ts to calc time saved when becomes cohead
//     else
//         read no dirty data, becomes cohead
//         record time saved from elr is 0.
//     		
#define UPDATE_RETIRE_INFO(en, prev) { \
  if (prev) { \
    if (prev->type == LOCK_SH) { \
      en->is_cohead = prev->is_cohead; \
      if (!en->is_cohead) { \
        en->start_ts = get_sys_clock(); \
        en->txn->increment_commit_barriers(); \
      } else { \
        record_benefit(0);} \
    } else { \
      en->is_cohead = false; \
      en->start_ts = get_sys_clock(); \
      en->txn->increment_commit_barriers(); } \
  } else { \
    record_benefit(0); \
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
// (2) NO NEED to update owners cohead information 
//     if owner is not cohead, it cannot become one with RD inserted
//     if owner is cohead, RD still cannot change its status
//     as WAR([RW]) does not form commit dependency
#define INSERT_TO_RETIRED_TAIL(to_insert) { \
  UPDATE_RETIRE_INFO(to_insert, retired_tail); \
  LIST_PUT_TAIL(retired_head, retired_tail, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; }


#define INSERT_TO_RETIRED(to_insert, en) { \
  UPDATE_RETIRE_INFO(to_insert, en->prev); \
  LIST_INSERT_BEFORE_CH(retired_head, en, to_insert); \
  to_insert->status = LOCK_RETIRED; \
  retired_cnt++; \
}

#define RETIRE_ENTRY(to_retire) { \
  to_retire = owners; \
  owners = NULL; \
  to_retire->next=NULL; \
  to_retire->prev=NULL; \
  ADD_TO_RETIRED_TAIL(to_retire); }

#define CHECK_ROLL_BACK(en) { \
    en->access->orig_row->copy(en->access->orig_data); \
  }

// try_wound(to_wound, wounder), if commited, wound failed, return wounder
#define TRY_WOUND(to_wound, wounder) { \
  if (wounder->txn->wound_txn(to_wound->txn) == COMMITED) {\
    return_entry(wounder); \
    rc = Abort; \
    goto final; \
  } \
  printf("[txn-%lu](%lu) wound %lu(%lu) on %p\n", wounder->txn->get_txn_id(), \
				  wounder->txn->get_ts(), to_wound->txn->get_txn_id(), to_wound->txn->get_ts(), this); \
}

struct BBLockEntry {
    // type of lock: EX or SH
    lock_t type;
    lock_status status;
    bool is_cohead;
    txn_man * txn;
    BBLockEntry * next;
    BBLockEntry * prev;
    Access * access;
    ts_t start_ts;
#if LATCH == LH_MCSLOCK
    mcslock::qnode_t * m_node;
    BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
                   txn(NULL), next(NULL), prev(NULL), access(NULL),
                   start_ts(0), m_node(NULL){};
#else
    BBLockEntry(): type(LOCK_NONE), status(LOCK_DROPPED), is_cohead(false),
    txn(NULL), next(NULL), prev(NULL), access(NULL) start_ts(0) {};
#endif
};

class Row_bamboo {
  public:
    void init(row_t * row);
    RC lock_get(lock_t type, txn_man * txn, Access * access);
    RC lock_release(void * en, RC rc);
    RC lock_retire(void * en);

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
    UInt32 benefit_cnt1;
    UInt32 benefit_cnt2;
    UInt32 benefit1;
    UInt32 benefit2;
    bool curr_benefit1;
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
    void              lock(BBLockEntry * en);
    void              unlock(BBLockEntry * en);
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
    }

    // init a lock entry (pre-allocated in each txn's access)
    BBLockEntry * get_entry(Access * access) {
        BBLockEntry * entry = (BBLockEntry *) access->lock_entry;
        entry->txn->lock_ready = false;
        entry->txn->lock_abort = false;
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
        entry->is_cohead = false;
        entry->start_ts = 0;
        return entry;
    };

    // clean the lock entry
    void return_entry(BBLockEntry * entry) {
        entry->next = NULL;
        entry->prev = NULL;
        entry->status = LOCK_DROPPED;
        entry->start_ts = 0;
    }

    // record benefit
    inline void record_benefit(uint64_t time) {
        if (curr_benefit1) {
            if (benefit_cnt1 == 10) {
                curr_benefit1 = false;
                if (benefit_cnt2 == 10) {
                    // clear benefit2
                    benefit2 = time;
                    benefit_cnt2 = 1;
                } else {
                    benefit2 += time;
                    benefit_cnt2 += 1;
                }
            } else {
                benefit1 += time;
                benefit_cnt1 += 1;
            }
        } else {
            if (benefit_cnt2 == 10) {
                curr_benefit1 = true;
                if (benefit_cnt1 == 10) {
                    // clear benefit1
                    benefit1 = time;
                    benefit_cnt1 = 1;
                } else {
                    benefit1 += time;
                    benefit_cnt1 += 1;
                }
            } else {
                benefit2 += time;
                benefit_cnt2 += 1;
            }
        };
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
	};	

	// NOTE: it is unrealistic to have completely ordered read with
	// dynamically assigned ts. e.g. [0,0,0] -> [12, 11, 5]
	// used when to_insert->type = LOCK_SH
	inline RC wound_retired_rd(ts_t ts, BBLockEntry * to_insert) {
		BBLockEntry * en = retired_head;
		while(en) {
			if (en->type == LOCK_EX && a_higher_than_b(ts, en->txn->get_ts())) {
				if (to_insert->txn->wound_txn(en->txn) == COMMITED) {
					return_entry(to_insert);
					return Abort;
				}
    			printf("[txn-%lu](%lu) wounded %lu(%lu) on %p\n", to_insert->txn->get_txn_id(), 
				  		to_insert->txn->get_ts(), en->txn->get_txn_id(), en->txn->get_ts(), this);
				en = rm_from_retired(en, true, to_insert->txn);
			} else 
				en = en->next;
		}
	};

	// try_wound(to_wound, wounder), if commited, wound failed, return wounder
	inline RC wound_retired_wr(ts_t ts, BBLockEntry * to_insert) {
		BBLockEntry * en = retired_head;
		while(en) {
			if (en->txn->get_ts() == 0 || a_higher_than_b(ts, en->txn->get_ts())) {
				if (to_insert->txn->wound_txn(en->txn) == COMMITED) {
					return_entry(to_insert);
					return Abort;
				}
    			printf("[txn-%lu](%lu) wounded %lu(%lu) on %p\n", to_insert->txn->get_txn_id(), 
				  		to_insert->txn->get_ts(), en->txn->get_txn_id(), en->txn->get_ts(), this);
				en = rm_from_retired(en, true, to_insert->txn);
			} else 
				en = en->next;
		}
		return RCOK;
	};

	inline RC wound_owner(BBLockEntry * to_insert) {
		if (to_insert->txn->wound_txn(owners->txn) == COMMITED) {
			return_entry(to_insert);
			return Abort;
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
