#include "row.h"
#include "txn.h"
#include "row_ww.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_ww::init(row_t * row) {
	_row = row;
	// owners is a single linked list, each entry/node contains info like lock type, prev/next
	owners = NULL;
    // waiter is a double linked list. two ptrs to the linked lists
	waiters_head = NULL;
	waiters_tail = NULL;
	owner_cnt = 0;
	waiter_cnt = 0;

	latch = new pthread_mutex_t;
	pthread_mutex_init(latch, NULL);
	
	lock_type = LOCK_NONE;
	blatch = false;
}

RC Row_ww::lock_get(lock_t type, txn_man * txn) {
	uint64_t *txnids = NULL;
	int txncnt = 0;
	return lock_get(type, txn, txnids, txncnt);
}

RC Row_ww::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt) {
	assert (CC_ALG == WOUND_WAIT);
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
	
	if (owner_cnt != 0) { 
		// Cannot be added to the owner list.
        ///////////////////////////////////////////////////////////
        //  - T is the txn currently running
        //  always can wait but need to abort txns has lower priority (larger ts)
        //////////////////////////////////////////////////////////

        // always can wait
        //bool canwait = true;
        // go through owners
        LockEntry * en = owners;
        while (en != NULL) {
	    if (en->txn->get_txn_id() == txn->get_txn_id()) {
		//already in owners
		// check lock type
		// same type grab
		if(type == LOCK_EX && lock_type == LOCK_SH && owner_cnt > 1) {
			en->prev->next = en->next;
			en->next->prev = en->prev;
			owner_cnt--;
		} else {
			if (type == LOCK_EX)
				en->type = LOCK_EX;
			txn->lock_ready = true;
			rc = RCOK;
			goto final;
		} 
	    }
            //else if (en->txn->get_ts() > txn->get_ts()) {
            else if ((en->txn->get_ts() > txn->get_ts()) && conflict_lock(en->type, type)) {
                // step 1 - figure out what need to be done when aborting a txn
                // ask thread to abort
                #if DEBUG_WW
			printf("[row_ww]txn %lu abort txn %lu\n", txn->get_txn_id(), en->txn->get_txn_id());
                #endif
                en->txn->abort_txn();
            }
            en = en->next;
        }

        // TODO: insert to wait list
        // insert txn to the right position
        // the waiter list is always in timestamp order
        LockEntry * entry = get_entry();
        entry->txn = txn;
        entry->type = type;
        en = waiters_head;
        while (en != NULL)
	{
	    if (en->txn->get_txn_id() == txn->get_txn_id()) {
		if (en->type == LOCK_SH)
			en->type = type;
		txn->lock_ready = false;
		rc = WAIT;
		goto final;
	    }
	    if (txn->get_ts() < en->txn->get_ts())
		break;
	    else 
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
        rc = WAIT;
		#if DEBUG_WW
			printf("[row_ww] add txn %lu to waiters of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		#endif


	} else {
		// if owner is empty, grab the lock
		LockEntry * entry = get_entry();
		entry->type = type;
		entry->txn = txn;
		STACK_PUSH(owners, entry);
		owner_cnt ++;
		lock_type = type;
		txn->lock_ready = true;
        	rc = RCOK;
		#if DEBUG_WW
			printf("[row_ww] add txn %lu to owners of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		#endif
	}


final:

	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return rc;
}


RC Row_ww::lock_release(txn_man * txn) {


	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	// Try to find the entry in the owners
	LockEntry * en = owners;
	LockEntry * prev = NULL;

	while (en != NULL && en->txn != txn) {
		prev = en;
		en = en->next;
	}
	if (en) { // find the entry in the owner list
		if (prev) prev->next = en->next;
		else owners = en->next;
		return_entry(en);
		owner_cnt --;
		if (owner_cnt == 0)
			lock_type = LOCK_NONE;
		#if DEBUG_WW
			printf("[row_ww] rm txn %lu from owners of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		#endif
	} else {
		// Not in owners list, try waiters list.
		en = waiters_head;
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
		#if DEBUG_WW
			printf("[row_ww] rm txn %lu from waiters of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		#endif
	}

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
		ASSERT(entry->txn->lock_ready == false);
		entry->txn->lock_ready = true;
		lock_type = entry->type;
		#if DEBUG_WW
			printf("[row_ww] bring %lu from waiter to owner to row %lu\n", entry->txn->get_txn_id(), _row->get_row_id());
		#endif
	} 
	ASSERT((owners == NULL) == (owner_cnt == 0));

	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );

	return RCOK;
}

bool Row_ww::conflict_lock(lock_t l1, lock_t l2) {
	if (l1 == LOCK_NONE || l2 == LOCK_NONE)
		return false;
    else if (l1 == LOCK_EX || l2 == LOCK_EX)
        return true;
	else
		return false;
}

LockEntry * Row_ww::get_entry() {
	LockEntry * entry = (LockEntry *) 
		mem_allocator.alloc(sizeof(LockEntry), _row->get_part_id());
	return entry;
}
void Row_ww::return_entry(LockEntry * entry) {
	mem_allocator.free(entry, sizeof(LockEntry));
}

