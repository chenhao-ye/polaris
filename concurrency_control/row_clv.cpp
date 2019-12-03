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
	
	lock_type = LOCK_NONE;
	blatch = false;
}

RC Row_clv::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_clv::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == CLV);
	RC rc;
    // get part id
	//int part_id =_row->get_part_id();
	if (g_central_man)
	    // if using central manager
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );
    // each thread has at most one owner of a lock
	assert(owner_cnt <= g_thread_cnt);
	// each thread has at most one waiter
	assert(waiter_cnt < g_thread_cnt);

#if DEBUG_ASSERT
	if (owners != NULL)
		assert(lock_type == owners->type); 
	else 
		assert(lock_type == LOCK_NONE);
	LockEntry * en = owners;
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

	// check lock type with owner
	// owner:
	// - SH then followers are SH
	// - EX then followers are NONE

	// bool conflict = conflict_lock(lock_type, type);

	// added one more condition for conflicts -- check wait dependency
	//if (!conflict) {
	    // TODO: waiters_head is not null and current txn's ts < waiter_head's ts -- conflict and need to wound them
	//	if (waiters_head && txn->get_ts() < waiters_head->txn->get_ts())
	//		conflict = true;
	//}

    if (owner_cnt + retired_cnt == 0) {
        // append txn to owners
        add_to_owner(type, txn);
        rc = RCOK;
#if DEBUG_CLV
        printf("[row_clv] add txn %lu to owners of row %lu\n", txn->get_txn_id(), _row->get_row_id());
#endif
    } else {
        bool can_acquire = (!conflict_lock(type, lock_type) && !violate(txn, waiters_head->txn));
        abort_or_dependent(retired, txn, true);
        if (can_acquire) {
            add_to_owner(type, txn);
            add_dependencies(txn, waiters_head);
            rc = RCOK;
        } else {
            abort_or_dependent(owners, txn, true);
            LockEntry * next = insert_to_waiter(type, txn);
            add_dependencies(txn, next);
            rc = WAIT;
        }
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
    LockEntry * en = remove_if_exists(owners, txn, true);
    assert(en != NULL);
    // append entry to retired
    STACK_PUSH(retired, en);
    retired_cnt ++;

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
	if (remove_if_exists(retired, txn, false) != NULL) {
#if DEBUG_CLV
        printf("[row_clv] rm txn %lu from retired of row %lu\n", txn->get_txn_id(), _row->get_row_id());
#endif
    } else if (remove_if_exists(owners, txn, true) != NULL) {
#if DEBUG_CLV
        printf("[row_clv] rm txn %lu from owner of row %lu\n", txn->get_txn_id(), _row->get_row_id());
#endif
	} else {
		// Not in owners list, try waiters list.
		LockEntry * en = waiters_head;
		while (en != NULL && en->txn != txn)
			en = en->next;
		ASSERT(en);
		LIST_REMOVE(en);
		if (en == waiters_head)
			waiters_head = en->next;
		if (en == waiters_tail)
			waiters_tail = en->prev;
		return_entry(en);
		waiter_cnt --;
		#if DEBUG_CLV
			printf("[row_clv] rm txn %lu from waiters of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		#endif
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
    if (owner_cnt == 0)
        ASSERT(lock_type == LOCK_NONE);
#if DEBUG_ASSERT && (CC_ALG == WAIT_DIE || CC_ALG == WOUND_WAIT)
    for (en = waiters_head; en != NULL && en->next != NULL; en = en->next)
			assert(en->next->txn->get_ts() < en->txn->get_ts());
#endif

    LockEntry * entry;
    // If any waiter can join the owners, just do it!
    while (waiters_head && !conflict_lock(lock_type, waiters_head->type)) {
        LIST_GET_HEAD(waiters_head, waiters_tail, entry);
        STACK_PUSH(owners, entry);
        owner_cnt ++;
        waiter_cnt --;
        ASSERT(entry->txn->lock_ready == 0);
        entry->txn->lock_ready = true;
        lock_type = entry->type;
#if DEBUG_CLV
        printf("[row_clv] bring %lu from waiter to owner to row %lu\n", entry->txn->get_txn_id(), _row->get_row_id());
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

bool Row_clv::violate(txn_man * high, txn_man * low) {
    if (low->get_ts() == 0) {
        if (high->get_ts() == 0) {
            high->set_ts(get_sys_clock());
        }
        low->set_ts(get_sys_clock());
    } else {
        if ((low->get_ts() == 0) || high->get_ts() > low->get_ts())
            return true;
    }
    return false;
}

void Row_clv::abort_or_dependent(LockEntry * list, txn_man * txn, bool high_first) {
    LockEntry * en = list;
    txn_man * high;
    txn_man * low;
    while(en != NULL) {
        if (high_first) {
            high = en->txn;
            low = txn;
        } else {
		high = txn;
		low = en->txn;
	}
        if (violate(high, low)) {
            high->abort_txn();
        } else {
            add_dependency(high, low);
        }
        en = en->next;
    }
}

void Row_clv::add_dependency(txn_man * high, txn_man * low) {
    if (high->add_descendants(low))
        low->increment_ancestors();
}

void
Row_clv::add_to_owner(lock_t type, txn_man * txn) {
    LockEntry * entry = get_entry();
    entry->type = type;
    entry->txn = txn;
    STACK_PUSH(owners, entry);
    owner_cnt ++;
    lock_type = type;
    txn->lock_ready = true;
}

LockEntry *
Row_clv::insert_to_waiter(lock_t type, txn_man * txn) {
    LockEntry * entry = get_entry();
    entry->txn = txn;
    entry->type = type;
    LockEntry * en = waiters_head;
    while (en != NULL)
    {
        if (txn->get_ts() < en->txn->get_ts())
            break;
        add_dependency(en->txn, txn);
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
    return en;
}

void
Row_clv::add_dependencies(txn_man * high, LockEntry * head) {
    LockEntry * en = head;
    while (en != NULL) {
        add_dependency(high, en->txn);
        en = en->next;
    }
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
            if (is_owner) owners = en->next;
            else retired = en->next;
        }
        return_entry(en);
        if (is_owner) {
            owner_cnt--;
            if (owner_cnt == 0)
                lock_type = LOCK_NONE;
        } else {
            retired_cnt--;
        }
#if DEBUG_CLV
        printf("[row_clv] rm txn %lu from owners of row %lu\n", txn->get_txn_id(), _row->get_row_id());
#endif
        return en;
    }
    return NULL;
}


