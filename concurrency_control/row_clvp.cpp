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
	retired = NULL;
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

void 
Row_clvp::clean_aborted(){
	CLVLockEntry * en = retired;
	CLVLockEntry * to_return = NULL;
	while(en) {
		if (en->txn->lock_abort) {
			// aborted, need to abort dependees
			to_return = remove_descendants(en);
			en = to_return->prev;
			//TODO return en 
			return_entry(to_return);
		} else {
			en = en->next;
		}
	}
	#if DEBUG_ASSERT
	debug();
	#endif
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

#if DEBUG_ASSERT
	debug();
#endif

	RC rc = WAIT;
	if ((check_abort(type, txn, retired) == Abort) || (check_abort(type, txn, retired) == Abort)) {
		rc = Abort;
		bring_next();
		goto final;
	}

	#if DEBUG_ASSERT
	debug();
	#endif

	insert_to_waiters(type, txn);
	bring_next();

	// if brought in owner return acquired lock
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
	// Try to find the entry in the owners and remove
	CLVLockEntry * entry = rm_if_in_owner(txn);
	if (entry == NULL) {
		// may be already wounded by others, or self is aborted
		assert(txn->status == ABORTED);
		rc = Abort;
		//goto final;
	}

	#if DEBUG_ASSERT
	debug();
	#endif

	if (rc != Abort) {
		// TODO: must clean out retired list before inserting!!
		clean_aborted();

		// increment barriers if conflict
		if (retired_tail) {
			if (conflict_lock(retired_tail->type, entry->type)) {
				entry->delta = true;
				txn->increment_commit_barriers();
			} else {
				entry->is_cohead = retired_tail->is_cohead;
			}
		} else {
			entry->is_cohead = true;
		}

		// append entry to retired
		RETIRED_LIST_PUT_TAIL(retired, retired_tail, entry);
		retired_cnt++;

	#if DEBUG_CLV
		printf("[row_clv] move txn %lu from owners to retired type %d of row %lu\n",
				txn->get_txn_id(), entry->type, _row->get_row_id());
	#endif

	#if DEBUG_ASSERT
		assert_in_list(retired, retired_tail, retired_cnt, entry->txn);
	#endif
	}

//final:
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

	// Try to find the entry in the retired
	CLVLockEntry * en;
	if (rc == Abort) {
		en = rm_if_in_list(txn, true, true);
	} else {
		en = rm_if_in_list(txn, true, false);
	}
	// try to rm in owners
	if (en) {
		return_entry(en);
	} else {
		// need to check owners
		en = rm_if_in_owner(txn);
		if (en) {
			return_entry(en);
		} else {
			en = rm_if_in_list(txn, false, false);
			if (en) {
				return_entry(en);
			}
		}
	}

	#if DEBUG_ASSERT
	debug();
	assert_notin_list(retired, retired_tail, retired_cnt, txn);
	assert_notin_list(waiters_head, waiters_tail, waiter_cnt, txn);
	#endif

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
Row_clvp::bring_next() {

	clean_aborted();
	CLVLockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head) {
		if ((owners == NULL) || (!conflict_lock(owners->type, waiters_head->type))) {
			LIST_GET_HEAD(waiters_head, waiters_tail, entry);

			#if DEBUG_ASSERT
			// aseert no conflicts
			if (has_conflicts_in_list(owners, entry))
				assert(false);
			#endif

			QUEUE_PUSH(owners, owners_tail, entry);

			#if DEBUG_ASSERT
			if (owner_cnt > 0)
				assert(!conflict_lock(owners->type, entry->type));
			#endif

			owner_cnt ++;
			waiter_cnt --;
			ASSERT(entry->txn->lock_ready == 0);
			entry->txn->lock_ready = true;

			#if DEBUG_CLV
			printf("[row_clv] bring %lu from waiters to owners of row %lu\n",
					entry->txn->get_txn_id(), _row->get_row_id());
			#endif
			#if DEBUG_ASSERT
			assert_in_list(owners, owners_tail, owner_cnt, entry->txn);
			assert_notin_list(waiters_head, waiters_tail, waiter_cnt, entry->txn);
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

RC
Row_clvp::check_abort(lock_t type, txn_man * txn, CLVLockEntry * list) {
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
			status = ERROR;
		}
		en = en->next;
	}
	return status;
}

CLVLockEntry * 
Row_clvp::remove_descendants(CLVLockEntry * en) {
	assert(en != NULL);
	CLVLockEntry * to_return = NULL;

	// 1. remove self
	// need to update entry as non-conflicting locks may not get aborted and need to update
	update_entry(en);
	LIST_RM(retired, retired_tail, en, retired_cnt);
	CLVLockEntry * prev = en;

	#if DEBUG_CLV
	printf("[row_clv] rm aborted txn %lu from retired of row %lu\n", 
			en->txn->get_txn_id(), _row->get_row_id());
	#endif
	#if DEBUG_ASSERT
	debug();
	assert_notin_list(retired, retired_tail, retired_cnt, en);
	#endif

	// 2. remove from next conflict till end
	en = en->next;
	// find next conflict
	while(en && (!conflict_lock_entry(prev, en))) {
		en = en->next;
	}

	#if DEBUG_ASSERT
	debug();
	#endif
	
	if (en) {
		// remove from conflict till very end
		LIST_RM_SINCE(retired, retired_tail, en);
	} else {
		// has no conflicting entry after removed entry
		// 3. if owners do not conflict with removed entry
		if (!conflict_lock_entry(prev, owners)) {
			//return_entry(prev);
			return prev;
		}
	}

	// 4. abort from next conflict (en) till end
	while(en) {
		to_return = en;
		en->txn->set_abort();
		// removed from list, no need to decrement barrier as it is no longer commit
		retired_cnt--;

		#if DEBUG_CLV
		printf("[row_clv] rm aborted txn %lu from retired of row %lu\n", 
			en->txn->get_txn_id(), _row->get_row_id());
		#endif

		en = en->next;
		return_entry(to_return);
	}
	// 5.need to abort all owners as well
	// owners should be all aborted and becomes empty
	while(owners) {
		en = owners;
		en->txn->set_abort();
		return_entry(en);
		owners = owners->next;
	}
	owners_tail = NULL;
	owners = NULL;
	owner_cnt = 0;
	return prev;
}

CLVLockEntry * 
Row_clvp::rm_if_in_owner(txn_man * txn) {
	#if DEBUG_ASSERT
	if(owners)
		assert_in_list(owners, owners_tail, owner_cnt, owners);
	#endif

	CLVLockEntry * en = owners;
	CLVLockEntry * prev = NULL;

	while (en != NULL) {
		if (en->txn->get_txn_id() == txn->get_txn_id()) {
			break;
		}
		prev = en;
		en = en->next;
	}
	
	if (en) { // find the entry in the retired list
		#if DEBUG_CLV
		printf("[row_clv] rm txn %lu from owners of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
		#endif
		QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
		#if DEBUG_ASSERT
		assert_notin_list(owners, owners_tail, owner_cnt, txn);
		#endif
		// find and return
		return en;
	}
	// did not find
	return NULL;
}

CLVLockEntry * 
Row_clvp::rm_if_in_list(txn_man * txn, bool is_retired=false, bool is_abort=true) {
	
	CLVLockEntry * en;
	if (is_retired)
		en = retired;
	else
		en = waiters_head;

	while (en != NULL) {
		if (en->txn->get_txn_id() == txn->get_txn_id()) {
			break;
		}
		en = en->next;
	}
	if (en) { // find the entry in the retired list
		if (is_retired) {
			if (is_abort) {
				en = remove_descendants(en);
			} else {
				// retired needs to update entry
				update_entry(en);
				LIST_RM(retired, retired_tail, en, retired_cnt);
				#if DEBUG_CLV
				printf("[row_clv] rm txn %lu from retired of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
				#endif
			}	
		}
		else {
			LIST_RM(waiters_head, waiters_tail, en, waiter_cnt);
			#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from waiters of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			#endif
		}
		// FINISH: find and removed
		return en;
	}
	// did not find, need to keep working
	return NULL;
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
			assert(en == retired);
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
	assert(retired || retired->is_cohead);
}

/* debug methods */

void
Row_clvp::debug() {
	CLVLockEntry * en;
	CLVLockEntry * prev = NULL;
	int cnt = 0;
	// check retired
	en = retired;
	while(en) {
		assert(prev == en->prev);
		cnt += 1;
		prev = en;
		en = en->next;
	}
	assert(prev == retired_tail);
	assert(cnt == retired_cnt);
	// check waiters
	cnt = 0;
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
	en = owner;
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

