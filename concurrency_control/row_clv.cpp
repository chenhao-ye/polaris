#include "row.h"
#include "txn.h"
#include "row_clv.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_clv::init(row_t * row) {
	_row = row;
	// owners is a single linked list, each entry/node contains info like lock type, prev/next
	owners = NULL;
    // waiter is a double linked list. two ptrs to the linked lists
	waiters_head = NULL;
	waiters_tail = NULL;
	// retired is a linked list, the next of tail is the head of owners
	retired = NULL;
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
	check_abort(type, txn, retired, false);
	check_abort(type, txn, owners, true);
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

    if (g_central_man)
        glob_manager->release_row(_row);
    else
        pthread_mutex_unlock( latch );

    return rc;
}

RC Row_clv::lock_retire(txn_man * txn) {
    if (g_central_man)
        glob_manager->lock_row(_row);
    else
        pthread_mutex_lock( latch );

    // Try to find the entry in the owners and remove
    LockEntry * entry = remove_if_exists(owners, txn, true);
    assert(entry != NULL);
    // append entry to retired
    STACK_PUSH(retired, entry);
    retired_cnt++;
#if DEBUG_CLV
	printf("[row_clv] move txn %lu from retired to owners of row %lu\n",
			txn->get_txn_id(), _row->get_row_id());
#endif
    // increment barriers
    if (retired_cnt > 1)
        txn->increment_commit_barriers();
    // bring next owners from waiters
    bring_next();

    if (g_central_man)
        glob_manager->release_row(_row);
    else
        pthread_mutex_unlock( latch );

    return RCOK;
}

RC Row_clv::lock_release(txn_man * txn) {
	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// Try to find the entry in the retired
	LockEntry * prev_head = retired;
	LockEntry * en = remove_if_exists(retired, txn, false);
	if (en != NULL) {
        if ((retired != prev_head) && (retired_cnt > 0)) {
            // becoming head always remove one barrier
            retired->txn->decrement_commit_barriers();
        }
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
    while (waiters_head && (owners == NULL || !conflict_lock(owners->type, waiters_head->type) )) {
        LIST_GET_HEAD(waiters_head, waiters_tail, entry);
        STACK_PUSH(owners, entry);
        owner_cnt ++;
        waiter_cnt --;
        ASSERT(entry->txn->lock_ready == 0);
        entry->txn->lock_ready = true;
#if DEBUG_CLV
        printf("[row_clv] bring %lu from waiters to owners of row %lu\n",
        		entry->txn->get_txn_id(), _row->get_row_id());
#endif
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
	LockEntry * entry = (LockEntry *) 
		mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
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
#if DEBUG_WW
	printf("[row_clv] add txn %lu type %d to waiters of row %lu\n",
			txn->get_txn_id(), type, _row->get_row_id());
#endif
}

void
Row_clv::check_abort(lock_t type, txn_man * txn, LockEntry * list, bool is_owner) {
    LockEntry * en = list;
    LockEntry * prev = NULL;
    bool has_conflict = false;
    while (en != NULL) {
        if (conflict_lock(en->type, type) && (en->txn->get_ts() > txn->get_ts() || txn->get_ts() == 0))
            has_conflict = true;
        if (has_conflict) {
            if (txn->get_ts() != 0) {
                // abort txn
                txn->wound_txn(en->txn);
#if DEBUG_CLV
				printf("[row_clv] txn %lu abort txn %lu\n",
			        		txn->get_txn_id(), en->txn->get_txn_id());
#endif
                // remove from retired/owner
                if (prev)
                    prev->next = en->next;
                else {
                    if (is_owner)
                        owners = en->next;
                    else
                        retired = en->next;
                }
                // update count
                if (is_owner) {
#if DEBUG_CLV
					printf("[row_clv] rm txn %lu from owners of row %lu\n",
			        		txn->get_txn_id(), _row->get_row_id());
#endif
					owner_cnt--;
				} else {
#if DEBUG_CLV
					printf("[row_clv] rm txn %lu from retired of row %lu\n",
			        		txn->get_txn_id(), _row->get_row_id());
#endif
					retired_cnt--;
				}

            }
            if (en->txn->get_ts() == 0)
                en->txn->set_next_ts();
        }
        prev = en;
        en = en->next;
    }
    if (has_conflict)
        txn->set_next_ts();

}

LockEntry *
Row_clv::remove_if_exists(LockEntry * list, txn_man * txn, bool is_owner) {
    LockEntry * en = list;
    LockEntry * prev = NULL;

    while (en != NULL && en->txn != txn) {
        prev = en;
        en = en->next;
    }
    if (en) { // find the entry in the retired list
        if (prev)
            prev->next = en->next;
        else {
		if (is_owner) {
			if (owners == en)
				owners = en->next;
		} else {
			if (retired == en)
				retired = en->next;
		}
        }
        if (is_owner) {
            owner_cnt--;
        } else {
            retired_cnt--;
        }

#if DEBUG_CLV
	if (is_owner)
        	printf("[row_clv] rm txn %lu from owners of row %lu\n", txn->get_txn_id(), _row->get_row_id());
	else
        	printf("[row_clv] rm txn %lu from retired of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		
#endif
        return en;
    }
    return NULL;
}




