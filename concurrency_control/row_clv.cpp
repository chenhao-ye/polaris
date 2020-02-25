#include "row.h"
#include "txn.h"
#include "row_clv.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_clv::init(row_t * row) {
	_row = row;
	// owners is a double linked list, each entry/node contains info like lock type, prev/next
	owners = NULL;
	owners_tail = NULL;
	// waiter is a double linked list. two ptrs to the linked lists
	waiters_head = NULL;
	waiters_tail = NULL;
	// retired is a linked list, the next of tail is the head of owners
	retired_head = NULL;
	retired_tail = NULL;
	owner_cnt = 0;
	waiter_cnt = 0;
	retired_cnt = 0;
	// a switch for retire
	#if THREAD_CNT == 1
	retire_on = false;
	#else
	retire_on = true;
	#endif
	// track retire cnt of most recent 5 retires
	#if CLV_RETIRE_OFF > 0
	retire_switch = CLV_RETIRE_OFF;
	#endif
	// local timestamp
	local_ts = -1;
	txn_ts = 0;

#if SPINLOCK
	latch = new pthread_spinlock_t;
	pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#else
	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#endif
	blatch = false;
	#if DEBUG_TMP
	vec = (CLVLockEntry **) mem_allocator.alloc(sizeof(CLVLockEntry *) * g_thread_cnt, _row->get_part_id());
	for (size_t i = 0; i < g_thread_cnt; i++) {
		vec[i] = NULL;
	}
	#endif
}

inline
void Row_clv::lock() {
	if (g_thread_cnt > 1) {
		if (g_central_man)
			glob_manager->lock_row(_row);
		else {
			#if SPINLOCK
			pthread_spin_lock( latch );
			#else
			pthread_mutex_lock( latch );
			#endif
		}
	}
}

inline
void Row_clv::unlock() {
	if (g_thread_cnt > 1) {
		if (g_central_man)
			glob_manager->release_row(_row);
		else {
			#if SPINLOCK
			pthread_spin_unlock( latch );
			#else
			pthread_mutex_unlock( latch );
			#endif
		}
	}
}

inline void Row_clv::batch_return(CLVLockEntry * to_return) {
	CLVLockEntry * en;
	while (to_return) {
		en = to_return;
		to_return = to_return->next;
		return_entry(en);
	}
}

inline void Row_clv::batch_wound(CLVLockEntry * to_wound) {
	CLVLockEntry * en;
	while (to_wound) {
		en = to_wound;
		to_wound = to_wound->next;
		if (en->wounded) {
			assert(en->txn->status != COMMITED);
			en->txn->set_abort();
		}
		return_entry(en);
	}
}

inline void Row_clv::reset_entry(CLVLockEntry * entry) {
	entry->txn = NULL;
	entry->type = LOCK_NONE;
	entry->prev = NULL;
	entry->next = NULL;
	entry->delta = false;
	entry->is_cohead = false;
	#if DEBUG_TMP
	entry->loc = LOC_NONE;
	#endif
}

RC Row_clv::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_clv::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == CLV);

	#if (THREAD_CNT > 1) && (DELAY_ACQUIRE > 0) && (DELAY_THRESHOLD < THREAD_CNT)
	if (( (int) retired_cnt > max(DELAY_THRESHOLD, THREAD_CNT / 2)) && owner_cnt != 0)
		usleep(DELAY_ACQUIRE);
	#endif

	CLVLockEntry * to_insert; 
	#if BATCH_RETURN_ENTRY
	CLVLockEntry * to_return = NULL; 
	#endif

	#if DEBUG_TMP
	uint64_t idx = txn->get_thd_id()%g_thread_cnt;
	to_insert = vec[idx]; 
	if (!to_insert)
	#endif
		to_insert = get_entry();

	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif
	lock();
	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
	starttime = get_sys_clock();
	#endif

	#if DEBUG_TMP
	if (!vec[idx])
		vec[idx] = to_insert;
	reset_entry(to_insert);
	#endif

	#if CLV_RETIRE_ON > 0
	// will turn back on at some time point
	if (!retire_on) {
		if (waiter_cnt > 0)
			retire_switch--;
		else
			retire_switch = min(CLV_RETIRE_ON, retire_switch+1);
		if  (retire_switch == 0) {
			//printf("turn retire back on, # waiters=%d!\n", waiter_cnt);
			retire_switch = CLV_RETIRE_OFF;
			retire_on = true;
		}
	}
	#endif

	// 1. set txn to abort in owners and retired
	RC rc = WAIT;
	RC status = RCOK;
	// if unassigned, grab or assign the largest possible number
	local_ts = -1;
	ts_t ts = txn->get_ts();
	txn_ts = ts;
	if (ts == 0) {
		// test if can grab the lock without assigning priority
		if ((waiter_cnt == 0) && 
				(retired_cnt == 0 || (!conflict_lock(retired_tail->type, type) && retired_tail->is_cohead)) && 
				(owner_cnt == 0 || !conflict_lock(owners->type, type)) ) {
			// add to owners directly
			to_insert->type = type;
			to_insert->txn = txn;
			txn->lock_ready = true;
			RETIRED_LIST_PUT_TAIL(owners, owners_tail, to_insert);
			#if DEBUG_TMP
			to_insert->loc = OWNERS;
			#endif
			owner_cnt++;
			unlock();
			return RCOK;
		}
		// else has to assign a priority and add to waiters first 
		// assert(retired_cnt + owner_cnt != 0);
		// heuristic to batch assign ts: 
		//int batch_n_ts = retired_cnt + owner_cnt + 1;
		
		int batch_n_ts = 1;
		if ( waiter_cnt == 0 ) {
			if (retired_tail && (retired_tail->txn->get_ts() == 0)) {
				batch_n_ts += retired_cnt;
			} 
			batch_n_ts += owner_cnt;	
		} 
		//local_ts = txn->set_next_ts(retired_cnt + owner_cnt + 1);
		local_ts = txn->set_next_ts(batch_n_ts);
		if (local_ts != 0) {
			// if != 0, already booked n ts. 
			txn_ts = local_ts;
			local_ts = local_ts - batch_n_ts + 1;
			//assert(txn->get_ts() != local_ts); // make sure did not change pointed addr
		} else {
			// if == 0, fail to assign, oops, self has an assigned number anyway
			ts = txn->get_ts();
			txn_ts = ts;
		}
	}

	// 2. wound conflicts
	// 2.1 check retired
	#if BATCH_RETURN_ENTRY
	status = wound_conflict(type, txn, ts, true, status, to_return);
	#else
	status = wound_conflict(type, txn, ts, true, status);
	#endif
	if (status == Abort) {
		rc = Abort;
		if (owner_cnt == 0)
			bring_next(NULL);
		unlock();
		return rc;
		#if !DEBUG_TMP
		return_entry(to_insert);
		#if BATCH_RETURN_ENTRY
		return_batch(to_return);
		#endif
		#endif
	}

	// 2.2 check owners
	#if BATCH_RETURN_ENTRY
	status = wound_conflict(type, txn, ts, false, status, to_return);
	#else
	status = wound_conflict(type, txn, ts, false, status);
	#endif
	if (status == Abort) {
		rc = Abort;
		if (owner_cnt == 0)
			bring_next(NULL);
		unlock();
		return rc;
		#if !DEBUG_TMP
		return_entry(to_insert);
		#if BATCH_RETURN_ENTRY
		return_batch(to_return);
		#endif
		#endif
	}

	// 2. insert into waiters and bring in next waiter
	to_insert->txn = txn;
	to_insert->type = type;
	CLVLockEntry * en = waiters_head;
	while (en != NULL) {
		//if (txn->get_ts() < en->txn->get_ts())
		if (txn_ts < en->txn->get_ts())
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
	#if DEBUG_TMP
	to_insert->loc = WAITERS;
	#endif
	waiter_cnt ++;
	txn->lock_ready = false;

	// bring next available to owner in case both are read
	if (bring_next(txn)) {
		rc = RCOK;
		#if DEBUG_TMP
		to_insert->loc = OWNERS;
		#endif
	}


	#if !RETIRE_READ
	if (retire_on && owners && (waiter_cnt > 0) && (owners->type == LOCK_SH)) {
		// if retire turned on and share lock is the owner
                // move to retired
                CLVLockEntry * to_retire = NULL;
                while (owners) {
                        to_retire = owners;
                        LIST_RM(owners, owners_tail, to_retire, owner_cnt);
                        to_retire->next=NULL;
                        to_retire->prev=NULL;
			//assert(to_retire->txn->get_ts() != 0);
                        // try to add to retired
                        if (retired_tail) {
                                if (conflict_lock(retired_tail->type, to_retire->type)) {
                                        // conflict with tail -> increment barrier for sure
                                        // default is_cohead = false
                                        to_retire->delta = true;
                                        to_retire->txn->increment_commit_barriers();
                                } else {
                                        // not conflict with tail ->increment if is not head
                                        to_retire->is_cohead = retired_tail->is_cohead;
                                        if (!to_retire->is_cohead)
                                                to_retire->txn->increment_commit_barriers();
                                }
                        } else {
                                to_retire->is_cohead = true;
                        }
                        RETIRED_LIST_PUT_TAIL(retired_head, retired_tail, to_retire);
                        retired_cnt++;
                }
                if (owner_cnt == 0 && bring_next(txn)) {
                        rc = RCOK;
                        #if DEBUG_TMP
                        to_insert->loc = OWNERS;
                        #endif
                }

	}
	#endif

	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - starttime);
	#endif
	unlock();
	#if !DEBUG_TMP
	#if BATCH_RETURN_ENTRY
	batch_return(to_return);
	#endif
	#endif
	return rc;
}

RC Row_clv::lock_retire(txn_man * txn) {


	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif
	if(!retire_on) {
		return RCOK;
	}
	lock();
	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug4, get_sys_clock() - starttime);
	starttime = get_sys_clock();
	#endif

	#if CLV_RETIRE_OFF > 0
	if (retired_cnt > 0)
		retire_switch = min(CLV_RETIRE_OFF, retire_switch + 1);
	else
		retire_switch--;
	if (retire_switch == 0) {
		//printf("turn retire off, set retire switch from %d to %d!\n", retire_switch, CLV_RETIRE_ON);
		#if CLV_RETIRE_ON > 0
		// if will turn back on
		retire_switch = CLV_RETIRE_ON;
		#endif
		retire_on = false;
	}
	#endif

	RC rc = RCOK;
	// 1. find entry in owner and remove
	#if !DEBUG_TMP
	CLVLockEntry * entry = owners;
	while (entry) {
		if (entry->txn == txn)
			break;
		entry = entry->next;
	}
	if (entry) {
	#else
	CLVLockEntry * entry = vec[txn->get_thd_id()%g_thread_cnt];
	if (entry->loc != LOC_NONE) {
	#endif
		// rm from owners
		LIST_RM(owners, owners_tail, entry, owner_cnt);
		entry->next=NULL;
		entry->prev=NULL;
		//assert(entry->txn->get_ts() != 0);
		// try to add to retired
		if (retired_tail) {
			if (conflict_lock(retired_tail->type, entry->type)) {
				// conflict with tail -> increment barrier for sure
				// default is_cohead = false
				entry->delta = true;
				entry->txn->increment_commit_barriers();
			} else { 
				// not conflict with tail ->increment if is not head
				entry->is_cohead = retired_tail->is_cohead;
				if (!entry->is_cohead)
					entry->txn->increment_commit_barriers();
			}
		} else {
			entry->is_cohead = true;
		}

		RETIRED_LIST_PUT_TAIL(retired_head, retired_tail, entry);
		retired_cnt++;
		#if DEBUG_TMP
		entry->loc = RETIRED;
		#endif
	} else {
		// may be is aborted
		//assert(txn->status == ABORTED);
		rc = Abort;
	}
	if (owner_cnt == 0)
		bring_next(NULL);

	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug5, get_sys_clock() - starttime);
	starttime = get_sys_clock();
	#endif

	unlock();
	return rc;
}

RC Row_clv::lock_release(txn_man * txn, RC rc) {

	#if DEBUG_TMP
	if (vec[txn->get_thd_id()%g_thread_cnt]->loc == LOC_NONE)
		return RCOK;
	#endif

	#if BATCH_RETURN_ENTRY
	CLVLockEntry * to_return = NULL;
	#endif

	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif
	lock();
	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug6, get_sys_clock() - starttime);
	starttime = get_sys_clock();
	#endif

	#if DEBUG_TMP
	// check out where it is 
	CLVLockEntry * en = vec[txn->get_thd_id()%g_thread_cnt];
	if (en->loc == LOC_NONE) {// already aborted
	} else if (en->loc == RETIRED) {
		#if BATCH_RETURN_ENTRY
		rm_if_in_retired(txn, rc == Abort, to_return);
		#else
		rm_if_in_retired(txn, rc == Abort);
		#endif
	} else if (en->loc == OWNERS) {
		LIST_RM(owners, owners_tail, en, owner_cnt);
		en->loc = LOC_NONE;
		en->next = NULL;
		en->prev = NULL;
	} else if (en->loc == WAITERS) {
		LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
		en->loc = LOC_NONE;
		en->next = NULL;
		en->prev = NULL;
	}
	#else
	CLVLockEntry * en = NULL;
	// Try to find the entry in the retired
	#if BATCH_RETURN_ENTRY
	if (!rm_if_in_retired(txn, rc == Abort, to_return)) {
	#else
	if (!rm_if_in_retired(txn, rc == Abort)) {
	#endif
		// Try to find the entry in the owners
		en = owners;
		while (en) {
			if (en->txn == txn)
				break;
			en = en->next;
		}
		if (en) {
			// rm from owners
			LIST_RM(owners, owners_tail, en, owner_cnt);
			#if BATCH_RETURN_ENTRY
			RETURN_PUSH(to_return, en);
			#endif
		} else {
			// not found in owner or retired, try waiters
			en = waiters_head;
			while(en) {
				if (en->txn == txn) {
					LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
					#if BATCH_RETURN_ENTRY
					RETURN_PUSH(to_return, en);
					#endif
					break;
				}
				en = en->next;
			}
		}
	}
	#endif

	if (owner_cnt == 0) {
		bring_next(NULL);
	}
	// WAIT - done releasing with is_abort = true
	// FINISH - done releasing with is_abort = false
	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug7, get_sys_clock() - starttime);
	#endif
	unlock();
	#if !DEBUG_TMP
	#if BATCH_RETURN_ENTRY
	batch_return(to_return);
	#else
	if (en)
		return_entry(en);
	#endif
	#endif
	return RCOK;
}

#if BATCH_RETURN_ENTRY
inline bool Row_clv::rm_if_in_retired(txn_man * txn, bool is_abort, CLVLockEntry *& to_return) {
#else
inline bool Row_clv::rm_if_in_retired(txn_man * txn, bool is_abort) {
#endif
	#if DEBUG_TMP
	CLVLockEntry * en = vec[txn->get_thd_id()%g_thread_cnt];
	if (en->loc != RETIRED)
		return false;
	else {
		if (is_abort) {
			en->txn->lock_abort = true;
			#if BATCH_RETURN_ENTRY
			en = remove_descendants(en, to_return, txn);
			#else
			en = remove_descendants(en, txn);
			#endif
		}
		else {
			assert(txn->status == COMMITED);
			update_entry(en);
			LIST_RM(retired_head, retired_tail, en, retired_cnt);
			en->loc = LOC_NONE;
			en->next = NULL;
			en->prev = NULL;
		}
		return true;
	}
	#else
	CLVLockEntry * en = retired_head;
	while(en) {
		if (en->txn == txn) {
			if (is_abort) {
				en->txn->lock_abort = true;
				#if BATCH_RETURN_ENTRY
				en = remove_descendants(en, to_return, txn);
				#else
				en = remove_descendants(en, txn);
				#endif
			} else {
				assert(txn->status == COMMITED);
				update_entry(en);
				LIST_RM(retired_head, retired_tail, en, retired_cnt);
				#if BATCH_RETURN_ENTRY
				RETURN_PUSH(to_return, en);
				#else
				return_entry(en);
				#endif
			}
			return true;
		} else 
			en = en->next;
	}
	return false;
	#endif
}

inline bool
Row_clv::bring_next(txn_man * txn) {
	bool has_txn = false;
	CLVLockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head) {
		if ((owner_cnt == 0) || (!conflict_lock(owners->type, waiters_head->type))) {
			LIST_GET_HEAD(waiters_head, waiters_tail, entry);
			waiter_cnt --;
			// add to onwers
			RETIRED_LIST_PUT_TAIL(owners, owners_tail, entry);
			#if DEBUG_TMP
			entry->loc = OWNERS;
			#endif
			owner_cnt ++;
			entry->txn->lock_ready = true;
			if (txn == entry->txn) {
				has_txn = true;
			}
			#if DEBUG_CLV
			printf("bring to owner thd=%lu, txn=%lu row=%lu\n", entry->txn->get_thd_id(), entry->txn->get_txn_id(), _row->get_row_id() );
			#endif
		} else
			break;
	}
	ASSERT((owners == NULL) == (owner_cnt == 0));
	return has_txn;
}


bool Row_clv::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
		else if (l1 == LOCK_EX || l2 == LOCK_EX)
			return true;
	else
		return false;
}

bool Row_clv::conflict_lock_entry(CLVLockEntry * l1, CLVLockEntry * l2) {
	if (l1 == NULL || l2 == NULL)
		return false;
	return conflict_lock(l1->type, l2->type);
}


CLVLockEntry * Row_clv::get_entry() {
	CLVLockEntry * entry = (CLVLockEntry *) mem_allocator.alloc(sizeof(CLVLockEntry), _row->get_part_id());
	entry->prev = NULL;
	entry->next = NULL;
	entry->delta = false;
	entry->is_cohead = false;
	entry->txn = NULL;
	entry->wounded = false;
	return entry;
}

void Row_clv::return_entry(CLVLockEntry * entry) {
	mem_allocator.free(entry, sizeof(CLVLockEntry));
}

#if BATCH_RETURN_ENTRY
inline bool Row_clv::wound_txn(CLVLockEntry * en, txn_man * txn, bool check_retired, CLVLockEntry *& to_return) {
#else
inline bool Row_clv::wound_txn(CLVLockEntry * en, txn_man * txn, bool check_retired) {
#endif

	if (txn->status == ABORTED)
		return false;
	if (en->txn->set_abort() != ABORTED)
		return false;
	if (check_retired) {
		#if BATCH_RETURN_ENTRY 
		en = remove_descendants(en, to_return, txn);
		#else
		en = remove_descendants(en, txn);
		#endif
	} else {
		LIST_RM(owners, owners_tail, en, owner_cnt);
		#if DEBUG_TMP
		en->loc = LOC_NONE;
		en->next = NULL;
		en->prev = NULL;
		#else
		#if BATCH_RETURN_ENTRY
		RETURN_PUSH(to_return, en);
		#else
		return_entry(en);
		#endif
		#endif
	}
	return true;
}

inline RC
#if BATCH_RETURN_ENTRY
Row_clv::wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status, CLVLockEntry *& to_return) {
#else
Row_clv::wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status) {
#endif
	CLVLockEntry * en;
	CLVLockEntry * to_reset;
	if (check_retired)
		en = retired_head;
	else
		en = owners;
	bool recheck = false;
	int checked_cnt = 0;
	while (en) {
		checked_cnt++;
		recheck = false;
		#if DEBUG_TMP
		assert(en->loc != LOC_NONE);
		#endif
		ts_t en_ts = en->txn->get_ts();
		if (ts != 0) {
			// self assigned, if conflicted, assign a number
			//if (status == RCOK && conflict_lock(en->type, type) && 
			//	 ((en_ts > txn->get_ts()) || (en_ts == 0))) {
			if (status == RCOK && conflict_lock(en->type, type) && 
				 ((en_ts > txn_ts) || (en_ts == 0))) {
				status = WAIT;
			}
			if (status == WAIT) {
				if ((en_ts > ts) || (en_ts == 0)) {
					to_reset = en;
					en = en->prev;
					//txn_man * tmp_txn = to_reset->txn;
					#if BATCH_RETURN_ENTRY
					if (!wound_txn(to_reset, txn, check_retired, to_return))
					#else
					if (!wound_txn(to_reset, txn, check_retired))
					#endif
						return Abort;
					if (en)
						en = en->next;
					else {
						if (check_retired)
							en = retired_head;
						else
							en = owners;
					}	
				} else {
					en = en->next;
				}
			} else {
				en = en->next;
			}
		} else {
			// if already commited, abort self
			if (en->txn->status == COMMITED) {
					en = en->next;
					continue;
			}
			// self unassigned, if not assigned, assign a number;
			if (en_ts == 0) {
				// if already commited, abort self
				if (en->txn->status == COMMITED) {
					en = en->next;
					continue;
				}
				assert(local_ts < txn_ts);
				if (!en->txn->atomic_set_ts(local_ts)) { // it has a ts already
					recheck = true;
				} else {
					en_ts = local_ts;
					local_ts++;
				}
			} 
			//if (!recheck && (en->txn->get_ts() > txn->get_ts())) {
			if (!recheck && (en_ts > txn_ts)) {
				to_reset = en;
				en = en->prev;
				#if BATCH_RETURN_ENTRY
				if (!wound_txn(to_reset, txn, check_retired, to_return))
				#else
				if (!wound_txn(to_reset, txn, check_retired))
				#endif
					return Abort;
				// if has previous
				if (en)
					en = en->next;
				else {
					if (check_retired)
						en = retired_head;
					else
						en = owners;
				}	
			} else {
				if (!recheck)
					en = en->next;
				else
					checked_cnt--;
			}
		}
	}
	/*
	if (retired_head && (type == LOCK_EX)) {
		if (txn->get_ts() <= retired_head->txn->get_ts() )
			printf("ts=%lu, txn-ts=%lu, head(%lu)-ts=%lu\n", ts, txn->get_ts(), retired_head->txn->get_txn_id(), retired_head->txn->get_ts());
		assert(txn->get_ts() > retired_tail->txn->get_ts());
	}
	*/
	return status;
}

inline void
Row_clv::insert_to_waiters(CLVLockEntry * entry, lock_t type, txn_man * txn) {
	assert(txn->get_ts() != 0);
	entry->txn = txn;
	entry->type = type;
	CLVLockEntry * en = waiters_head;
	while (en != NULL)
	{
		if (txn->get_ts() < en->txn->get_ts())
			break;
		en = en->next;
	}
	if (en) {
		LIST_INSERT_BEFORE(en, entry);
		if (en == waiters_head)
			waiters_head = entry;
	} else {
		LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
	}
	waiter_cnt ++;
	txn->lock_ready = false;
}


#if BATCH_RETURN_ENTRY
inline CLVLockEntry * 
Row_clv::remove_descendants(CLVLockEntry * en, CLVLockEntry *& to_return, txn_man * txn) {
#else
inline CLVLockEntry *
Row_clv::remove_descendants(CLVLockEntry * en, txn_man * txn) {
#endif
	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	uint32_t abort_cnt = 1;
	uint32_t abort_try = 1;
	#endif
	assert(en != NULL);
	CLVLockEntry * next = NULL;
	CLVLockEntry * prev = en->prev;
	// 1. remove self, set iterator to next entry
	lock_t type = en->type;
	bool conflict_with_owners = conflict_lock_entry(en, owners);
	next = en->next;
	if (type == LOCK_SH) {
		// update entry and no need to remove descendants!
		update_entry(en);
		LIST_RM(retired_head, retired_tail, en, retired_cnt);
		#if !DEBUG_TMP
        	#if BATCH_RETURN_ENTRY
        	RETURN_PUSH(to_return, en);
        	#else
        	return_entry(en);
        	#endif
        	#else
        	en->loc = LOC_NONE;
        	en->next = NULL;
        	en->prev = NULL;
        	#endif
		if (prev)
			return prev->next;
		else
			return retired_head;
	}
	//update_entry(en); // no need to update as any non-cohead needs to be aborted, coheads will not be aborted
	LIST_RM(retired_head, retired_tail, en, retired_cnt);
	#if !DEBUG_TMP
	#if BATCH_RETURN_ENTRY
	RETURN_PUSH(to_return, en);
	#else 
	return_entry(en);
	#endif
	#else
	en->loc = LOC_NONE;
	en->next = NULL;
	en->prev = NULL;
	#endif
	en = next;
	// 2. remove next conflict till end
	// 2.1 find next conflict
	while(en && (!conflict_lock(type, en->type))) {
		en = en->next;
	}
	// 2.2 remove dependees
	if (en == NULL) {
		if (!conflict_with_owners) {
			// clean owners
			while(owners) {
				en = owners;
				// no need to be too complicated (i.e. call function) as the owner will be empty in the end
				owners = owners->next;
				#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
				abort_try++;
				if (en->txn->status == ABORTED)
					abort_cnt++;
				else
				#endif
					en->txn->set_abort();
				#if !DEBUG_TMP
				#if BATCH_RETURN_ENTRY
				RETURN_PUSH(to_return, en);
				#else 
				return_entry(en);
				#endif
				#else
				en->loc = LOC_NONE;
				en->next = NULL;
				en->prev = NULL;
				#endif
			}
			owners_tail = NULL;
			owners = NULL;
			owner_cnt = 0;
		} // else, nothing to do
	} else {
		// abort till end
		LIST_RM_SINCE(retired_head, retired_tail, en);
		while(en) {
			next = en->next;
			#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
			abort_try++;
			if (en->txn->status == ABORTED)
				abort_cnt++;
			else
			#endif
				en->txn->set_abort();
			retired_cnt--;
			#if !DEBUG_TMP
			#if BATCH_RETURN_ENTRY
			RETURN_PUSH(to_return, en);
			#else 
			return_entry(en);
			#endif
			#else
			en->loc = LOC_NONE;
			en->next = NULL;
			en->prev = NULL;
			#endif
			en = next;
		}
	}
	assert(!retired_head || retired_head->is_cohead);
	#if DEBUG_PROFILING && CLV_DEBUG_PROFILING
	// debug9: sum of all lengths of chains; debug 10: time of cascading aborts; debug2: max chain
	if (abort_cnt > 1) {
		INC_STATS(0, debug2, 1);
		INC_STATS(0, debug9, abort_cnt); // out of all aborts, how many are cascading aborts (have >= 1 dependency)
	}
	// max length of aborts
	if (abort_cnt > stats._stats[txn->get_thd_id()]->debug11)
		stats._stats[txn->get_thd_id()]->debug11 = abort_cnt;
	// max length of depedency
	if (abort_try > stats._stats[txn->get_thd_id()]->debug10)
		stats._stats[txn->get_thd_id()]->debug10 = abort_try;
	#endif
	if (prev)
		return prev->next;
	else
		return retired_head;
}


inline void
Row_clv::update_entry(CLVLockEntry * en) {
	CLVLockEntry * entry;
	if (en->prev) {
		if (en->next) {
			if (en->delta && !en->next->delta) // WR(1)R(0)
				en->next->delta = true;
		} else {
			// has no next, nothing needs to be updated
		}
	} else {
		// has no previous, en = head
		if (en->next) {
			#if DEBUG_ASSERT
			assert(en == retired_head);
			#endif
			// has next entry
			// en->next->is_cohead = true;
			if (!en->next->is_cohead) {
				en->next->delta = false;
				entry = en->next;
				while(entry && (!entry->delta)) {
					assert(!entry->is_cohead);
					entry->is_cohead = true;
					entry->txn->decrement_commit_barriers();
					entry = entry->next;
				}
			} // else (R)RR, no changes
			assert(en->next->is_cohead);
		} else {
			// has no next entry, never mind
		}
	}
}
