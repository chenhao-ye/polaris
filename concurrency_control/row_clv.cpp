#include "row.h"
#include "txn.h"
#include "row_clv.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_clv::init(row_t * row) {
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

RC Row_clv::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_clv::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == CLV);
	RC rc = WAIT;
	LockEntry * en;

	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// each thread has at most one owner of a lock
	assert(owner_cnt <= g_thread_cnt);
	// each thread has at most one waiter
	assert(waiter_cnt < g_thread_cnt);

#if DEBUG_ASSERT
	en = owners;
	UInt32 cnt = 0;
	while (en) {
		assert(en->txn->get_thd_id() != txn->get_thd_id());
		cnt ++;
		en = en->next;
	}
	assert(cnt == owner_cnt);
	en = waiters_head;
	cnt = 0;
	while (en) {
		cnt ++;
		en = en->next;
	}
	assert(cnt == waiter_cnt);
#endif

	RC status = RCOK;
	txn->lock_ts();
	status = check_abort(type, txn, retired, false, status == WAIT);

	if (status == ERROR) {
		rc = Abort;
		bring_next();
		goto final;
	} 

	status = check_abort(type, txn, owners, true, status == WAIT);
	if (status == ERROR) {
		rc = Abort;
		bring_next();
		goto final;
	}
	if ((status == WAIT) && (txn->get_ts() == 0))
		txn->set_next_ts();
	txn->unlock_ts();

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

RC Row_clv::lock_retire(txn_man * txn) {

	RC rc = RCOK;

	if (g_central_man)
		glob_manager->lock_row(_row);
	else
		pthread_mutex_lock( latch );

	// Try to find the entry in the owners and remove
	LockEntry * entry = remove_if_exists(owners, txn, true);
	if (entry == NULL) {
		// may be already wounded by others
		assert(txn->status == ABORTED);
		rc = Abort;
		goto final;
	}
	// append entry to retired
	entry->next = NULL;
	QUEUE_PUSH(retired, retired_tail, entry);
	retired_cnt++;
#if DEBUG_CLV
	printf("[row_clv] move txn %lu from owners to retired type %d of row %lu\n",
			txn->get_txn_id(), entry->type, _row->get_row_id());
	assert_in_list(retired, retired_tail, retired_cnt, entry->txn);
#endif
	// increment barriers if conflict
	if ((retired_cnt > 1) && has_conflicts_in_list(retired, entry))
		txn->increment_commit_barriers();
	

final:
	// bring next owners from waiters
	bring_next();
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}

RC Row_clv::lock_release(txn_man * txn) {
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// Try to find the entry in the retired
	LockEntry * en = remove_if_exists(retired, txn, false);
	if (en != NULL) {
			return_entry(en);
	 } else {
		en = remove_if_exists(owners, txn, true);

		if (en != NULL) {
			return_entry(en);
		} else {
				// Not in owners list, try waiters list.
				LockEntry *en = waiters_head;
			 	while (en != NULL && en->txn != txn)
					en = en->next;
				if (en) {
					LIST_REMOVE(en);
					if (en == waiters_head)
							waiters_head = en->next;
					if (en == waiters_tail)
							waiters_tail = en->prev;
					return_entry(en);
					waiter_cnt--;
					#if DEBUG_CLV
					printf("[row_clv] rm txn %lu from waiters of row %lu\n", txn->get_txn_id(), _row->get_row_id());
					assert_notin_list(waiters_head, waiters_tail, waiter_cnt, txn);
					#endif
				}
		}
	}
	
	bring_next();

	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return RCOK;
}

void
Row_clv::bring_next() {

	LockEntry * entry;
	// If any waiter can join the owners, just do it!
	while (waiters_head) {
		if ((owners == NULL) || (!conflict_lock(owners->type, waiters_head->type))) {
			LIST_GET_HEAD(waiters_head, waiters_tail, entry);
			#if DEBUG_CLV
			// aseert no conflicts
			if (has_conflicts_in_list(owners, entry))
				assert(false);
			#endif
			QUEUE_PUSH(owners, owners_tail, entry);
			#if DEBUG_CLV
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
			assert_in_list(owners, owners_tail, owner_cnt, entry->txn);
			assert_notin_list(waiters_head, waiters_tail, waiter_cnt, entry->txn);
			#endif
		} else
			break;
	}
	ASSERT((owners == NULL) == (owner_cnt == 0));
}

bool Row_clv::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
		else if (l1 == LOCK_EX || l2 == LOCK_EX)
			return true;
	else
		return false;
}

LockEntry * Row_clv::get_entry() {
	LockEntry * entry = (LockEntry *) mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
	entry->next = NULL;
	return entry;
}

void Row_clv::return_entry(LockEntry * entry) {
	mem_allocator.free(entry, sizeof(LockEntry));
}

void
Row_clv::insert_to_waiters(lock_t type, txn_man * txn) {
	LockEntry * entry = get_entry();
	entry->txn = txn;
	entry->type = type;
	LockEntry * en = waiters_head;
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
	} else
		LIST_PUT_TAIL(waiters_head, waiters_tail, entry);
	waiter_cnt ++;
	txn->lock_ready = false;
#if DEBUG_CLV
	printf("[row_clv] add txn %lu type %d to waiters of row %lu\n",
			txn->get_txn_id(), type, _row->get_row_id());
	assert_in_list(waiters_head, waiters_tail, waiter_cnt, txn);
#endif
}

RC
Row_clv::check_abort(lock_t type, txn_man * txn, LockEntry * list, bool is_owner, bool has_conflict) {
	LockEntry * en = list;
	LockEntry * prev = NULL;
	LockEntry * prev_head = NULL;
	while (en != NULL) {
		en->txn->lock_ts();
		if (conflict_lock(en->type, type) && (en->txn->get_ts() > txn->get_ts() || txn->get_ts() == 0))
			has_conflict = true;
		if (has_conflict) {
			if (txn->get_ts() != 0) {
				// abort txn
				if (txn->wound_txn(en->txn) == ERROR) {
					#if DEBUG_CLV
					printf("[row_clv] detected txn %lu is aborted when "
					"trying to wound others on row %lu\n", txn->get_txn_id(),  _row->get_row_id());
					#endif
					return Abort;
				}
				#if DEBUG_CLV
				printf("[row_clv] txn %lu abort txn %lu on row %lu\n", txn->get_txn_id(), en->txn->get_txn_id(), _row->get_row_id());
				#endif

				if (is_owner) {
					#if DEBUG_CLV
					printf("[row_clv] txn %lu rm another txn %lu from owners of row %lu\n", txn->get_txn_id(), en->txn->get_txn_id(), _row->get_row_id());
					#endif
					QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
					assert_notin_list(owners, owners_tail, owner_cnt, en->txn);
				} else {
					#if DEBUG_CLV
					printf("[row_clv] txn %lu rm another txn %lu from retired of row %lu\n", txn->get_txn_id(), en->txn->get_txn_id(), _row->get_row_id());
					#endif
					prev_head = retired;
					QUEUE_RM(retired, retired_tail, prev, en, retired_cnt);
					if ((retired_cnt > 0) && (retired != prev_head) && (conflict_lock(retired->type, prev_head->type)))
						retired->txn->decrement_commit_barriers();
					assert_notin_list(retired, retired_tail, retired_cnt, en->txn);
				}
			}
			if (en->txn->get_ts() == 0)
				en->txn->set_next_ts();

		} else
			prev = en;
		en->txn->unlock_ts();
		en = en->next;
	}
	if (has_conflict) {
		return WAIT;
	}
	return RCOK;
}

LockEntry *
Row_clv::remove_if_exists(LockEntry * list, txn_man * txn, bool is_owner) {
	LockEntry * en = list;
	LockEntry * prev = NULL;
	LockEntry * prev_head = NULL;

	while (en != NULL && en->txn->get_txn_id() != txn->get_txn_id()) {
		prev = en;
		en = en->next;
	}
	if (en) { // find the entry in the retired list
		if (is_owner) {
			#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from owners of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			#endif
			QUEUE_RM(owners, owners_tail, prev, en, owner_cnt);
			assert_notin_list(owners, owners_tail, owner_cnt, txn);
		} else {
			#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from retired of row %lu\n", en->txn->get_txn_id(), _row->get_row_id());
			#endif
			prev_head = retired;
			QUEUE_RM(retired, retired_tail, prev, en, retired_cnt);
			// coalescing two parts if they are not conflicted, and one part has head
			if ((retired_cnt >= 2) && )
				;
			else if ((retired_cnt > 0) && (retired != prev_head) && (conflict_lock(retired->type, en->type)))
				retired->txn->decrement_commit_barriers();
			assert_notin_list(retired, retired_tail, retired_cnt, txn);
		}
		return en;
	}
	return NULL;
}

void
Row_clv::print_list(LockEntry * list, LockEntry * tail, int cnt) {
	LockEntry * en = list;
	int count = 0;
	while(en){
		printf("(%lu, %d) -> ", en->txn->get_txn_id(), en->type);
		en = en->next;
		count += 1;
	}
	printf("expected cnt: %d, real cnt: %d, expected tail: %lu\n", cnt, count, 
		tail->txn->get_txn_id());
}


void
Row_clv::assert_notin_list(LockEntry * list, LockEntry * tail, int cnt, txn_man * txn) {
	LockEntry * en = list;
	LockEntry * prev = NULL;
	int count = 0;
	while(en){
		assert(txn->get_txn_id() != en->txn->get_txn_id());
		prev = en;
		en = en->next;
		count += 1;
	}
	assert(count == cnt);
	assert(tail == prev);
}

void
Row_clv::assert_in_list(LockEntry * list, LockEntry * tail, int cnt, txn_man * txn) {
	LockEntry * en = list;
	LockEntry * prev = NULL;
	int count = 0;
	bool in = false;
	while(en){
		if(txn->get_txn_id() == en->txn->get_txn_id())
			in = true;
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
Row_clv::has_conflicts_in_list(LockEntry * list, LockEntry * entry) {
	LockEntry * en;
	en = list;
	while(en) {
		if (conflict_lock(en->type, entry->type)) {
			return true;
		}
		en = en->next;
	}
	return false;
}

