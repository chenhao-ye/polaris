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
	
	#if DEBUG_TMP
		printf("[row_ww] %lu got mutex in lock_get %lu\n", txn->get_txn_id(), _row->get_row_id());
	#endif

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

	
	if (owner_cnt == 0) {
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
			printf("[row_ww] add txn %lu type %d to owners of row %lu\n", txn->get_txn_id(), type, _row->get_row_id());
		#endif
	} else {
        	LockEntry * en = owners;
        	LockEntry * prev = NULL;
        	while (en != NULL) {
	        	if (en->txn->get_ts() > txn->get_ts() && conflict_lock(lock_type, type)) {
                		// step 1 - figure out what need to be done when aborting a txn
                		// ask thread to abort
                	#if DEBUG_WW
			        printf("[row_ww]txn %lu abort txn %lu\n", txn->get_txn_id(), en->txn->get_txn_id());
                	#endif
                		txn->wound_txn(en->txn);
                		// remove from owner
                		if (prev)
                    			prev->next = en->next;
                		else
                    			owners = en->next;
                		// update count
                		owner_cnt--;
				if (owner_cnt == 0)
					lock_type = LOCK_NONE;
            		}
            	en = en->next;
	        prev = en;
        }

        // insert to wait list
        // insert txn to the right position
        // the waiter list is always in timestamp order
        LockEntry * entry = get_entry();
        entry->txn = txn;
        entry->type = type;
        en = waiters_head;
        while ((en != NULL) && (txn->get_ts() > en->txn->get_ts()))
            en = en->next;
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
        printf("[row_ww] add txn %lu type %d to waiters of row %lu\n", txn->get_txn_id(), type, _row->get_row_id());
#endif

        bring_next();
#if DEBUG_TMP
        printf("[row_ww] txn %lu tried to take waiters to owners of row %lu\n", txn->get_txn_id(), _row->get_row_id());
#endif

        // if brought in owner return acquired lock
        en = owners;
        while(en){
            if (en->txn == txn) {
                rc = RCOK;
#if DEBUG_TMP
        	printf("[row_ww] txn %lu is brought to owners of row %lu\n", txn->get_txn_id(),  _row->get_row_id());
#endif
                break;
            }
		en = en->next;
        }
	}

	#if DEBUG_TMP
		printf("[row_ww] %lu try to release mutex in lock_get %lu\n", txn->get_txn_id(), _row->get_row_id());
	#endif
	if (g_central_man)
		glob_manager->release_row(_row);
	else
		pthread_mutex_unlock( latch );
	#if DEBUG_TMP
		printf("[row_ww] %lu release mutex in lock_get %lu\n", txn->get_txn_id(), _row->get_row_id());
	#endif

	return rc;
}


RC Row_ww::lock_release(txn_man * txn) {

	#if DEBUG_TMP
		printf("[row_ww] try to acquire mutex in lock_release %lu held by %lu\n", _row->get_row_id(), txn->get_txn_id());
	#endif


	if (g_central_man)
		glob_manager->lock_row(_row);
	else 
		pthread_mutex_lock( latch );

	#if DEBUG_TMP
		printf("[row_ww] got mutex in lock_release %lu held by %lu\n", _row->get_row_id(), txn->get_txn_id());
	#endif
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
		printf("[row_ww] not found txn %lu to release in owners, try waiters of row %lu\n", txn->get_txn_id(), _row->get_row_id());
		// Not in owners list, try waiters list.
		en = waiters_head;
		while (en != NULL && en->txn != txn)
			en = en->next;
		if (en) {
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
	}

	bring_next();
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

void
Row_ww::bring_next() {
    LockEntry * entry;
    // If any waiter can join the owners, just do it!
    while (waiters_head && (owners == NULL || !conflict_lock(owners->type, waiters_head->type) )) {
        LIST_GET_HEAD(waiters_head, waiters_tail, entry);
        STACK_PUSH(owners, entry);
        owner_cnt ++;
        waiter_cnt --;
        ASSERT(entry->txn->lock_ready == 0);
        entry->txn->lock_ready = true;
#if DEBUG_WW
        printf("[row_ww] bring %lu from waiter to owner of row %lu\n", entry->txn->get_txn_id(), _row->get_row_id());
#endif
    }
    ASSERT((owners == NULL) == (owner_cnt == 0));
}
