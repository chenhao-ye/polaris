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
	retire_on = false;
	// local timestamp
	local_ts = -1;

#if SPINLOCK
	latch = new pthread_spinlock_t;
	pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#else
	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
#endif
	blatch = false;
	retire_on = false;
	#if DEBUG_TMP
	vec = (CLVLockEntry **) mem_allocator.alloc(sizeof(CLVLockEntry *) * g_thread_cnt, _row->get_part_id());
	for (size_t i = 0; i < g_thread_cnt; i++) {
		vec[i] = (CLVLockEntry *) mem_allocator.alloc(sizeof(CLVLockEntry), _row->get_part_id());
		reset_entry(vec[i]);
	}
	#endif
}

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

	#if !DEBUG_TMP
	CLVLockEntry * to_insert = get_entry();
	#endif

	#if DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif
	lock();
	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug1, get_sys_clock() - starttime);
	starttime = get_sys_clock();
	#endif

	// each thread has at most one owner of a lock
	assert(owner_cnt <= g_thread_cnt);
	// each thread has at most one waiter
	assert(waiter_cnt < g_thread_cnt);

	#if DEBUG_TMP
	CLVLockEntry * to_insert = vec[txn->get_thd_id()%g_thread_cnt];
	reset_entry(to_insert);
	#endif

	// 1. set txn to abort in owners and retired
	RC rc = WAIT;
	RC status = RCOK;
	// if unassigned, grab or assign the largest possible number
	local_ts = -1;
	ts_t ts = txn->get_ts();
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
			rc = RCOK;
			goto final;
		}
		// else has to assign a priority and add to waiters first 
		assert(retired_cnt + owner_cnt != 0);
		local_ts = txn->set_next_ts(retired_cnt + owner_cnt + 1);
		if (local_ts != 0) {
			// if != 0, already booked n ts. 
			local_ts = local_ts - (retired_cnt + owner_cnt);
			assert(txn->get_ts() != local_ts); // make sure did not change pointed addr
		} else {
			// if == 0, fail to assign, oops, self has an assigned number anyway
			ts = txn->get_ts();
		}
	}

	// 2. wound conflicts
	// 2.1 check retired
	status = wound_conflict(type, txn, ts, true, status);
	if (status == Abort) {
		rc = Abort;
		bring_next(NULL);
		return_entry(to_insert);
		goto final;
	}

	// 2.2 check owners
	status = wound_conflict(type, txn, ts, false, status);
	if (status == Abort) {
		rc = Abort;
		bring_next(NULL);
		return_entry(to_insert);
		goto final;
	}

	// 2. insert into waiters and bring in next waiter
	to_insert->txn = txn;
	to_insert->type = type;
	CLVLockEntry * en = waiters_head;
	while (en != NULL) {
		if (txn->get_ts() < en->txn->get_ts())
			break;
		en = en->next;
	}
	if (en) {
		LIST_INSERT_BEFORE(en, entry);
		if (en == waiters_head)
			waiters_head = entry;
	} else {
		LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
	}
	#if DEBUG_TMP
	to_insert->loc = WAITERS;
	#endif
	waiter_cnt ++;
	txn->lock_ready = false;

	// turn on retire only when needed
	#if THREAD_CNT > 1
	if (!retire_on && waiter_cnt >= CLV_RETIRE_ON)
		retire_on = true;
	#endif

	if (bring_next(txn)) {
		rc = RCOK;
		#if DEBUG_TMP
		to_insert->loc = OWNERS;
		#endif
	}

	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug3, get_sys_clock() - starttime);
	#endif

final:
	unlock();
	return rc;
}

RC Row_clv::lock_retire(txn_man * txn) {

	#if DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif
	if(!retire_on) {
		return RCOK;
	}
	lock();
	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug4, get_sys_clock() - starttime);
	starttime = get_sys_clock();
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
		#if DEBUG_CLV
		printf("retire row thd=%lu, txn=%lu row=%lu\n", txn->get_thd_id(), txn->get_txn_id(), _row->get_row_id());
		#endif
		#if DEBUG_TMP
		entry->loc = RETIRED;
		#endif
	} else {
		// may be is aborted
		assert(txn->status == ABORTED);
		rc = Abort;
	}
	if (owner_cnt == 0)
		bring_next(NULL);

	#if DEBUG_PROFILING
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

	#if DEBUG_PROFILING
	uint64_t starttime = get_sys_clock();
	#endif
	lock();
	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug6, get_sys_clock() - starttime);
	starttime = get_sys_clock();
	#endif

	#if DEBUG_TMP
	// check out where it is 
	CLVLockEntry * en = vec[txn->get_thd_id()%g_thread_cnt];
	if (en->loc == LOC_NONE) {// already aborted
	} else if (en->loc == RETIRED) {
		rm_if_in_retired(txn, rc == Abort);
	} else if (en->loc == OWNERS) {
		LIST_RM(owners, owners_tail, en, owner_cnt);
		reset_entry(en);
	} else if (en->loc == WAITERS) {
		LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
		reset_entry(en);
	}
	#else
	// Try to find the entry in the retired
	if (!rm_if_in_retired(txn, rc == Abort)) {
		// Try to find the entry in the owners
		CLVLockEntry * en = owners;
		while (en) {
			if (en->txn == txn)
				break;
			en = en->next;
		}
		if (en) {
			// rm from owners
			LIST_RM(owners, owners_tail, en, owner_cnt);
		} else {
			// not found in owner or retired, try waiters
			en = waiters_head;
			while(en) {
				if (en->txn == txn) {
					LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
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
	#if DEBUG_PROFILING
	INC_STATS(txn->get_thd_id(), debug7, get_sys_clock() - starttime);
	#endif
	unlock();
	#if !DEBUG_TMP
	if (en)
		return_entry(en);
	#endif
	return RCOK;
}

bool
Row_clv::rm_if_in_retired(txn_man * txn, bool is_abort) {
	CLVLockEntry * en = retired_head;
	while(en) {
		if (en->txn == txn) {
			if (is_abort) {
				en = remove_descendants(en);
			} else {
				assert(txn->status == COMMITED);
				update_entry(en);
				LIST_RM(retired_head, retired_tail, en, retired_cnt);
				return_entry(en);
			}
			return true;
		} else 
			en = en->next;
	}
	return false;
}

bool 
Row_clv::rm_if_in_waiters(txn_man * txn) {
	CLVLockEntry * en = waiters_head;
	while(en) {
		if (en->txn == txn) {
			LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
			return_entry(en);
			return true;
		}
		en = en->next;
	}
	return false;
}


CLVLockEntry * 
Row_clv::rm_from_retired(CLVLockEntry * en) {
	CLVLockEntry * to_return = en->next;
	update_entry(en);
	LIST_RM(retired_head, retired_tail, en, retired_cnt);
	return_entry(en);
	return to_return;
}

bool
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
	return entry;
}

void Row_clv::return_entry(CLVLockEntry * entry) {
	mem_allocator.free(entry, sizeof(CLVLockEntry));
}

RC
Row_clv::wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status) {
	CLVLockEntry * en;
	CLVLockEntry * to_reset;
	if (check_retired)
		en = retired_head;
	else
		en = owners;
	bool recheck = false;
	while (en) {
		recheck = false;
		if (ts != 0) {
			// self assigned, if conflicted, assign a number
			if (status == RCOK && conflict_lock(en->type, type) && 
				 (en->txn->get_ts() > txn->get_ts() || en->txn->get_ts() == 0))
				status = WAIT;
			if (status == WAIT) {
				if (en->txn->get_ts() > ts || en->txn->get_ts() == 0) {
					if (txn->wound_txn(en->txn) == COMMITED) {
						return Abort;
					}
					if (check_retired)
						en = remove_descendants(en);
					else {
						to_reset = en;
						LIST_RM(owners, owners_tail, en, owner_cnt);
						en = en->next;
						#if DEBUG_TMP
						reset_entry(to_reset);
						#else
						return_entry(to_reset);
						#endif
					}
				} else {
					en = en->next;
				}
			} else {
				en = en->next;
			}
		} else {
			// self unassigned, if not assigned, assign a number;
			if (en->txn->get_ts() == 0) {
				if (!en->txn->atomic_set_ts(local_ts)) // it has a ts already
					recheck = true;
				else 
					local_ts++;
			} 
			if (!recheck && (en->txn->get_ts() > txn->get_ts())) {
				if (txn->wound_txn(en->txn) == COMMITED) {
					return Abort;
				}
				if (check_retired)
					en = remove_descendants(en);
				else {
					to_reset = en;
					LIST_RM(owners, owners_tail, en, owner_cnt);
					en = en->next;
					#if DEBUG_TMP
					reset_entry(to_reset);
					#else
					return_entry(to_reset);
					#endif
				}
			} else {
				en = en->next;
			}
		}
	}
	return status;
}

void
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


CLVLockEntry * 
Row_clv::remove_descendants(CLVLockEntry * en) {
	assert(en != NULL);
	CLVLockEntry * next = NULL;
	CLVLockEntry * prev = en->prev;
	// 1. remove self, set iterator to next entry
	lock_t type = en->type;
	bool conflict_with_owners = conflict_lock_entry(en, owners);
	next = en->next;
	update_entry(en);
	LIST_RM(retired_head, retired_tail, en, retired_cnt);
	#if !DEBUG_TMP
	return_entry(en);
	#else
	reset_entry(en);
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
				en->txn->set_abort();
				// no need to be too complicated (i.e. call function) as the owner will be empty in the end
				owners = owners->next;
				#if !DEBUG_TMP
				return_entry(en);
				#else
				reset_entry(en);
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
			en->txn->set_abort();
			retired_cnt--;
			#if !DEBUG_TMP
			return_entry(en);
			#else
			reset_entry(en);
			#endif
			en = next;
		}
	}
	if (prev)
		return prev->next;
	else
		return retired_head;
}


void
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
