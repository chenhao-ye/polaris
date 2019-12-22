#include "row.h"
#include "txn.h"
#include "row_clvp.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_clvp::init(row_t * row) {
	_row = row;
	// owners is a single linked list, each entry/node contains info like lock type, prev/next
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

	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);

	blatch = false;
}

RC Row_clvp::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_clvp::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == CLV);
	CLVLockEntry * en;

	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// each thread has at most one owner of a lock
	assert(owner_cnt <= g_thread_cnt);
	// each thread has at most one waiter
	assert(waiter_cnt < g_thread_cnt);

	// 1. set txn to abort in owners and retired

	RC rc = WAIT;
	if ((wound_conflict(type, txn, retired_head) == Abort) || (wound_conflict(type, txn, owners) == Abort)) {
		rc = Abort;
		bring_next();
		goto final;
	}

	// 2. insert into waiters and bring in next waiter
	insert_to_waiters(type, txn);
	bring_next();

	// 3. if brought txn in owner, return acquired lock
	en = owners;
	while(en){
		if (en->txn == txn) {
			rc = RCOK;
			break;
		}
		en = en->next;
	}

final:
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}

RC Row_clvp::lock_retire(txn_man * txn) {

	if (g_central_man)
		glob_manager->lock_row(_row);
	else
		pthread_mutex_lock( latch );

	RC rc = RCOK;

	// 1. find entry in owner and remove
	CLVLockEntry * entry = rm_if_in_owners(txn);
	if (entry == NULL) {
		// may be is aborted
		assert(txn->status == ABORTED);
		rc = Abort;
	}

	// 2. if txn not aborted, try to add to retired
	if (rc != Abort) {
		// 2.1 must clean out retired list before inserting!!
		clean_aborted_retired();
		// 2.2 increment barriers if conflicts with tail
		if (retired_tail) {
			if (conflict_lock(retired_tail->type, entry->type)) {
				// default is_cohead = false
				entry->delta = true;
				txn->increment_commit_barriers();
			} else 
				entry->is_cohead = retired_tail->is_cohead;
		} else 
			entry->is_cohead = true;
		// 2.3 append entry to retired
		RETIRED_LIST_PUT_TAIL(retired_head, retired_tail, entry);
		retired_cnt++;

	#if DEBUG_CLV
		printf("[row_clv] move txn %lu from owners to retired type %d of row %lu\n",
				txn->get_txn_id(), entry->type, _row->get_row_id());
	#endif
	#if DEBUG_ASSERT
		debug();
		assert_in_list(retired_head, retired_tail, retired_cnt, entry->txn);
	#endif
	}

	// bring next owners from waiters
	bring_next();

	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}

RC Row_clvp::lock_release(txn_man * txn, RC rc) {
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	CLVLockEntry * en;
	
	// Try to find the entry in the retired
	if (!rm_if_in_retired(txn, rc == Abort)) {
		// Try to find the entry in the owners
		en = rm_if_in_owners(txn);
		if (en) {
			return_entry(en);
		} else {
			rm_if_in_waiters(txn);
		}
	}

	// WAIT - done releasing with is_abort = true
	// FINISH - done releasing with is_abort = false
	bring_next();

	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return RCOK;
}


void
Row_clvp::clean_aborted_retired() {
	CLVLockEntry * en = retired_head;
	while(en) {
		if (en->txn->lock_abort) {
			en = remove_descendants(en);
		} else {
			en = en->next;
		}
	}
	#if DEBUG_ASSERT
	debug();
	#endif
}

void 
Row_clvp::clean_aborted_owner() {
	CLVLockEntry * en = owners;
	CLVLockEntry * prev = NULL;
	while (en) {
		if (en->txn->lock_abort) {
			// no changes to prev
			en = rm_from_owners(en, prev);
		} else {
			prev = en;
			en = en->next;
		}
	}
	#if DEBUG_ASSERT
	debug();
	#endif
}

CLVLockEntry * 
Row_clvp::rm_if_in_owners(txn_man * txn) {
	// NOTE: will not destroy entry
	CLVLockEntry * en = owners;
	CLVLockEntry * prev = NULL;
	while (en) {
		if (en->txn == txn)
			break;
		prev = en;
		en = en->next;
	}
	if (en) {
		rm_from_owners(en, prev, false);
		#if DEBUG_ASSERT
		debug();
		#endif
	}
	return en;
}

bool
Row_clvp::rm_if_in_retired(txn_man * txn, bool is_abort) {
	CLVLockEntry * en = retired_head;
	while(en) {
		if (en->txn == txn) {
			if (is_abort) {
				en = remove_descendants(en);
			} else {
				assert(txn->status == COMMITED);
				en = rm_from_retired(en);
			}
			#if DEBUG_ASSERT
			debug();
			assert_notin_list(retired_head, retired_tail, retired_cnt, txn);
			#endif
			return true;
		} else {
			en = en->next;
		}
	}
	return false;
}

bool 
Row_clvp::rm_if_in_waiters(txn_man * txn) {
	CLVLockEntry * en = waiters_head;
	while(en) {
		if (en->txn == txn) {
			LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
			#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from waiters of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			#endif
			return_entry(en);
			#if DEBUG_ASSERT
			debug();
			assert_notin_list(waiters_head, waiters_tail, waiter_cnt, txn);
			#endif
			return true;
		}
		en = en->next;
	}
	return false;
}


CLVLockEntry * 
Row_clvp::rm_from_owners(CLVLockEntry * en, CLVLockEntry * prev, bool destroy) {
	CLVLockEntry * to_return = en->next;
	QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
	if (destroy) {
		// return next entry
		return_entry(en);
	}
	#if DEBUG_CLV
	printf("[row_clv] rm txn %lu from owners of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
	#endif
	#if DEBUG_ASSERT
	debug();
	#endif
	// return removed entry
	return to_return;
}

CLVLockEntry * 
Row_clvp::rm_from_retired(CLVLockEntry * en) {
	CLVLockEntry * to_return = en->next;
	update_entry(en);
	LIST_RM(retired_head, retired_tail, en, retired_cnt);
	return_entry(en);
	#if DEBUG_ASSERT
	debug();
	assert_notin_list(waiters_head, waiters_tail, waiter_cnt, en->txn);
	#endif
	return to_return;
}

void
Row_clvp::bring_next() {

	clean_aborted_retired();
	clean_aborted_owner();

	CLVLockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head) {
		if ((owners == NULL) || (!conflict_lock(owners->type, waiters_head->type))) {
			LIST_GET_HEAD(waiters_head, waiters_tail, entry);
			waiter_cnt --;

			if (entry->txn->lock_abort) {
				continue;
			}
			
			// add to onwers
			QUEUE_PUSH(owners, owners_tail, entry);

			owner_cnt ++;
			ASSERT(entry->txn->lock_ready == 0);
			entry->txn->lock_ready = true;

			#if DEBUG_CLV
			printf("[row_clv] bring %lu from waiters to owners of row %lu\n",
					entry->txn->get_txn_id(), _row->get_row_id());
			#endif
		} else
			break;
	}
	ASSERT((owners == NULL) == (owner_cnt == 0));
}


bool Row_clvp::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
		else if (l1 == LOCK_EX || l2 == LOCK_EX)
			return true;
	else
		return false;
}

bool Row_clvp::conflict_lock_entry(CLVLockEntry * l1, CLVLockEntry * l2) {
	if (l1 == NULL || l2 == NULL)
		return false;
	return conflict_lock(l1->type, l2->type);
}


CLVLockEntry * Row_clvp::get_entry() {
	CLVLockEntry * entry = (CLVLockEntry *) mem_allocator.alloc(sizeof(CLVLockEntry), _row->get_part_id());
	entry->prev = NULL;
	entry->next = NULL;
	entry->delta = false;
	entry->is_cohead = false;
	entry->txn = NULL;
	return entry;
}

void Row_clvp::return_entry(CLVLockEntry * entry) {
	mem_allocator.free(entry, sizeof(CLVLockEntry));
}


RC
Row_clvp::wound_conflict(lock_t type, txn_man * txn, CLVLockEntry * list) {
	CLVLockEntry * en = list;
	RC status = RCOK;
	while (en != NULL) {
		if (conflict_lock(en->type, type) && (en->txn->get_ts() > txn->get_ts()) ) {
			if (txn->wound_txn(en->txn) == ERROR) {
				#if DEBUG_CLV
				printf("[row_clv] detected txn %lu is aborted when "
				"trying to wound others on row %lu\n", txn->get_txn_id(),  _row->get_row_id());
				#endif
				return Abort;
			}
			#if DEBUG_CLV
			printf("[row_clv] txn %lu abort txn %lu on row %lu\n", txn->get_txn_id(), 
				en->txn->get_txn_id(), _row->get_row_id());
			#endif
		}
		en = en->next;
	}
	return status;
}

void
Row_clvp::insert_to_waiters(lock_t type, txn_man * txn) {
	CLVLockEntry * entry = get_entry();
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

#if DEBUG_CLV
	printf("[row_clv] add txn %lu type %d to waiters of row %lu\n",
			txn->get_txn_id(), type, _row->get_row_id());
#endif
#if DEBUG_ASSERT
	assert_in_list(waiters_head, waiters_tail, waiter_cnt, txn);
#endif
}


CLVLockEntry * 
Row_clvp::remove_descendants(CLVLockEntry * en) {
	assert(en != NULL);
	CLVLockEntry * to_destroy = NULL;
	CLVLockEntry * prev = en->prev;
	// 1. remove self, set iterator to next entry
	lock_t type = en->type;
	bool conflict_with_owners = conflict_lock_entry(en, owners);
	en = rm_from_retired(en);

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
				owners = owners->next;
				return_entry(en);
			}
			owners_tail = NULL;
			owners = NULL;
			owner_cnt = 0;
		} // else, nothing to do
	} else {
		// abort till end
		LIST_RM_SINCE(retired_head, retired_tail, en);
		while(en) {
			to_destroy = en;
			#if DEBUG_CLV
			printf("[row_clv] rm aborted txn %lu from retired of row %lu\n", 
				en->txn->get_txn_id(), _row->get_row_id());
			#endif
			en->txn->set_abort();
			retired_cnt--;
			en = en->next;
			return_entry(to_destroy);
		}
	}
	if (prev)
		return prev->next;
	else
		return retired_head;
}


void
Row_clvp::update_entry(CLVLockEntry * en) {
	CLVLockEntry * entry;
	if (en->prev) {
		if (en->next) {
			if (en->next->delta == true) {
				if (!conflict_lock_entry(en->prev, en->next)) {
					// both are SH
					en->next->delta = false;
					// change delta, need to check cohead
					if (en->prev->is_cohead) {
						entry = en->next;
						while(entry && (entry->delta == false)) {
							entry->is_cohead = true;
							entry->txn->decrement_commit_barriers();
							entry = entry->next;
						}
					} // else, not cohead, nothing to do
				}
			} else {
				en->next->delta = en->delta;
			}
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
			if (en->next->delta) {
				en->next->delta = false;
				entry = en->next;
				while(entry && (entry->delta == false)) {
					entry->is_cohead = true;
					entry->txn->decrement_commit_barriers();
					entry = entry->next;
				}
			} // else (R)RR, no changes
		} else {
			// has no next entry, never mind
		}
	}
	assert(retired_head || retired_head->is_cohead);
}

/* debug methods */

void
Row_clvp::debug() {
	CLVLockEntry * en;
	CLVLockEntry * prev = NULL;
	UInt32 cnt = 0;
	// check retired
	bool has_conflicts = false;
	en = retired_head;
	while(en) {
		assert(prev == en->prev);
		if (conflict_lock_entry(prev, en)) {
			assert(en->delta);
			has_conflicts = true;
		}
		if (en != retired_head) {
			if (!conflict_lock_entry(retired_head, en)) {
				if (!has_conflicts) {
					assert(en->is_cohead);
				} else {
					assert(!(en->is_cohead));
				}
			} else {
				assert(!(en->is_cohead));
			}
		} else {
			assert(en->is_cohead);
			assert(!en->delta);
		}
		cnt += 1;
		prev = en;
		en = en->next;
	}
	assert(prev == retired_tail);
	assert(cnt == retired_cnt);
	// check waiters
	cnt = 0;
	prev = NULL;
	en = waiters_head;
	while(en) {
		assert(prev == en->prev);
		cnt += 1;
		prev = en;
		en = en->next;
	}
	assert(prev == waiters_tail);
	assert(cnt == waiter_cnt);
	// check owner
	cnt = 0;
	prev = NULL;
	en = owners;
	while(en) {
		cnt += 1;
		prev = en;
		en = en->next;
	}
	assert(prev == owners_tail);
	assert(cnt == owner_cnt);
}

void
Row_clvp::print_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt) {
	CLVLockEntry * en = list;
	int count = 0;
	while(en){
		printf("(%lu, %d) -> ", en->txn->get_txn_id(), en->type);
		en = en->next;
		count += 1;
	}
	if (tail) {
		printf("expected cnt: %d, real cnt: %d, expected tail: %lu\n", cnt, count, 
		tail->txn->get_txn_id());
	} else {
		printf("expected cnt: %d, real cnt: %d, expected tail is null\n", cnt, count);
	}
}


void
Row_clvp::assert_notin_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	int count = 0;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id())
			printf("ERROR: %lu is already in row %lu\n", txn->get_txn_id(), _row->get_row_id());
		assert(txn->get_txn_id() != en->txn->get_txn_id());
		prev = en;
		en = en->next;
		count += 1;
	}
	if (count != cnt){
		print_list(list, tail, cnt);
	}
	assert(count == cnt);
	assert(tail == prev);
}

void
Row_clvp::assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt, txn_man * txn) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	int count = 0;
	bool in = false;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id()) {
			if (in) {
				print_list(owners, owners_tail, owner_cnt);
				assert(false);
			}
			in = true;
		}
		prev = en;
		en = en->next;
		count += 1;
	}
	#if DEBUG_CLV
	if (tail != prev)
		print_list(list, tail, cnt);
	#endif
	// assert(tail->txn->get_txn_id() == txn->get_txn_id());
	assert(in);
	assert(tail == prev);
	assert(count == cnt);
}

void
Row_clvp::assert_in_list(CLVLockEntry * list, CLVLockEntry * tail, int cnt,CLVLockEntry * l) {
	CLVLockEntry * en = list;
	CLVLockEntry * prev = NULL;
	txn_man * txn = l->txn;
	int count = 0;
	bool in = false;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id()) {
			if (in) {
				print_list(owners, owners_tail, owner_cnt);
				assert(false);
			}
			in = true;
		}
		prev = en;
		en = en->next;
		count += 1;
	}
	#if DEBUG_CLV
	if (tail != prev)
		print_list(list, tail, cnt);
	#endif
	// assert(tail->txn->get_txn_id() == txn->get_txn_id());
	assert(in);
	assert(tail == prev);
	assert(count == cnt);
}

bool
Row_clvp::has_conflicts_in_list(CLVLockEntry * list, CLVLockEntry * entry) {
	CLVLockEntry * en;
	en = list;
	while(en) {
		if (conflict_lock(en->type, entry->type)) {
			return true;
		}
		en = en->next;
	}
	return false;
}

