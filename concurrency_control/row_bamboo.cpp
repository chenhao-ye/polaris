#include "row.h"
#include "txn.h"
#include "row_bamboo.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_bamboo::init(row_t * row) {
    _row = row;
    // owners contains at most one lock entry, whose type is always LOCK_EX
    owners = NULL;
    // waiter is a doubly linked list
    waiters_head = NULL;
    waiters_tail = NULL;
    // retired is a doubly linked list
    retired_head = NULL;
    retired_tail = NULL;
    waiter_cnt = 0;
    retired_cnt = 0;
    // init latches
#if LATCH == LH_SPINLOCK
    latch = new pthread_spinlock_t;
    pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#elif LATCH == LH_MUTEX
    latch = new pthread_mutex_t;
    pthread_mutex_init(latch, NULL);
#else
    latch = new mcslock();
#endif
    blatch = false;
    benefit1 = 0;
    benefit_cnt1 = 1;
    benefit2 = 0;
    benefit_cnt2 = 0;
    curr_benefit1 = true;

}

// taking the latch
void Row_bamboo::lock(BBLockEntry * en) {
    if (g_thread_cnt > 1) {
            if (g_central_man)
                glob_manager->lock_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_lock( latch );
#else
                latch->acquire(en->m_node);
#endif
            }
    }
};

// release the latch
void Row_bamboo::unlock(BBLockEntry * en) {
        if (g_thread_cnt > 1) {
            if (g_central_man)
                glob_manager->release_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_unlock( latch );
#else
                latch->release(en->m_node);
#endif
            }
        }
};

// Return value: RCOK/FINISH/WAIT/Abort
// - RCOK: acquired the lock and should read current data
// - FINISH: acquired the lock and have read previous copy
// - WAIT: added to wait list but not acquired the data
// - Abort: aborted
RC Row_bamboo::lock_get(lock_t type, txn_man * txn, Access * access) {
    // init return value
    RC rc = RCOK;
    // iterating helper
    BBLockEntry * en;
    // initialize a lock entry
    BBLockEntry * to_insert = get_entry(access);
    to_insert->type = type;
    // take the latch
    lock(to_insert);
    // timestamp
    ts_t ts = txn->get_ts();
    ts_t owner_ts = 0;
    ts_t en_ts;
#if !BB_DYNAMIC_TS
    // [pre-assigned ts] assign ts if does not have one
    if (ts == 0) {
        txn->set_next_ts(1);
        ts = txn->get_ts();
    }
#endif
    if (type == LOCK_SH) {
        // if read, decide if need to wait
        // need_to_wait(): ts > owner & owner is write.
        if (owners) {
            // get owner's timestamp
            owner_ts = owners->txn->get_ts();
#if BB_DYNAMIC_TS
            // the only writer in owner may be unassigned
            owner_ts = assign_ts(owner_ts, owners->txn);
            // self may be unassigned
            ts = assign_ts(ts, txn);
#endif
            if (a_higher_than_b(owner_ts, ts)) { // owner has higher priority
                // add to waiters
                ADD_TO_WAITERS(en, to_insert);
                goto final; // since owner is blocking others
            } else {
                // if has any retired, retired must have ts since there's owner
#if BB_OPT_RAW
				rc = insert_read_to_retired(to_insert, ts, access);
				goto final;
#else
                // wound owners
                WOUND_OWNER(to_insert);
                // wound and add to waiters (rc=WAIT)
                WOUND_RETIRED(en, to_insert);
                ADD_TO_WAITERS(en, to_insert);
#endif
            }
        } else { // no owners
#if BB_DYNAMIC_TS
            // no owner, retired may not have ts.
            // if all read, then no need for ts. only case to have self=0
            // if any writes, then it has to be just one write in the retired
            // list, with no other reads; then may need to assign ts
            if (retired_tail && (retired_tail->type == LOCK_EX)) {
                assign_ts(0, retired_tail->txn);
                ts = assign_ts(ts, txn);
            }
#endif
#if BB_OPT_RAW
			rc = insert_read_to_retired(to_insert, ts, access);
			goto final;
#else
            // no owner, wound and add to retired (rc=RCOK)
            WOUND_RETIRED(en, to_insert);
            UPDATE_RETIRE_INFO(to_insert, retired_tail);
            ADD_TO_RETIRED_TAIL(to_insert);
            goto final; // no owner -> no waiter, no need to promote
#endif
        }
    } else { // LOCK_EX
        // grab directly, no ts needed
        if (retired_cnt == 0 && !owners) {
            owners = to_insert;
            owners->status = LOCK_OWNER;
            UPDATE_RETIRE_INFO(to_insert, retired_tail);
            goto final;
        }
#if BB_DYNAMIC_TS
        // assign ts
        if (owners)
            owner_ts = owners->txn->get_ts();
        if (ts == 0) {
            if (owners) {
                if (!retired_head) {
                    // [ ][W][...]
                    owner_ts = assign_ts(owner_ts, owners->txn);
                    ts = assign_ts(ts, txn);
                } else {
                    // [...][W], everybody is assigned
                    ts = assign_ts(ts, txn);
                }
            } else {
                if (retired_head) {
                    // [RR/W][][]
                    en = retired_head;
                    for (UInt32 i = 0; i < retired_cnt; i++) {
                        assign_ts(0, en->txn);
                        en = en->next;
                    }
                    ts = assign_ts(ts, txn);
                } // else [][][] -> no need to assign self
            }
        }
#endif
        // wound retired
        en = retired_head;
        for (UInt32 i = 0; i < retired_cnt; i++) {
            if (a_higher_than_b(ts, en->txn->get_ts())) {
                TRY_WOUND(en, to_insert);
                en = rm_from_retired(en, true, txn);
            } else
                en = en->next;
        }
        // wound owners
        if (owners && a_higher_than_b(ts, owner_ts))
            WOUND_OWNER(to_insert);
        // add self to waiters
        ADD_TO_WAITERS(en, to_insert);
    }
    // promote waiters, need to decide whether to read dirty data
    if (bring_next(txn)) {
        rc = RCOK;
    } else {
        goto final;
    }
final:
#if DEBUG_BAMBOO
	printf("[txn-%lu] lock_get(%p, %d) status=%d ts=%lu\n", txn->get_txn_id(), this, type, rc, ts);
#endif
    // release the latch
    unlock(to_insert);
    if (rc == RCOK || rc == FINISH)
        txn->lock_ready = true;
    return rc;
}

RC Row_bamboo::lock_retire(void * addr) {
    BBLockEntry * entry = (BBLockEntry *) addr;
    ASSERT(entry->type == LOCK_EX);
#if PF_CS
    uint64_t starttime = get_sys_clock();
#endif
    lock(entry);
#if PF_CS
    uint64_t endtime = get_sys_clock();
    INC_STATS(entry->txn->get_thd_id(), time_retire_latch, endtime - starttime);
    starttime = endtime;
#endif
    RC rc = RCOK;
    // 1. find entry in owner and remove
    if (entry->status == LOCK_OWNER) {
        // move to retired list
        RETIRE_ENTRY(entry);
        // make dirty data globally visible
        if (entry->type == LOCK_EX) {
#if PF_CS
            uint64_t startt = get_sys_clock();
            entry->access->orig_row->copy(entry->access->data);
            INC_STATS(entry->txn->get_thd_id(), time_copy, get_sys_clock() - startt);
#else
            entry->access->orig_row->copy(entry->access->data);
#endif
        }
    } else {
        // may be is aborted: assert(txn->status == ABORTED);
        assert(entry->status == LOCK_DROPPED);
        rc = Abort;
    }
    bring_next(NULL);

#if PF_CS
    INC_STATS(entry->txn->get_thd_id(), time_retire_cs, get_sys_clock() -
    starttime);
#endif
#if DEBUG_BAMBOO
	printf("[txn-%lu] lock_retire(%p, %d) ts=%lu\n", entry->txn->get_txn_id(), this, entry->type, entry->txn->get_ts());
#endif
    unlock(entry);
    return rc;
}

RC Row_bamboo::lock_release(void * addr, RC rc) {
    BBLockEntry * entry = (BBLockEntry *) addr;
	if (entry->status == LOCK_DROPPED)
			return RCOK;
#if PF_ABORT 
    entry->txn->abort_chain = 0;
#endif
#if PF_CS
    uint64_t starttime = get_sys_clock();
#endif
    lock(entry);
#if PF_CS
    uint64_t endtime = get_sys_clock();
  INC_STATS(entry->txn->get_thd_id(), time_release_latch, endtime- starttime);
  starttime = endtime;
#endif
    // if in retired
    if (entry->status == LOCK_RETIRED) {
        rm_from_retired(entry, rc == Abort, entry->txn);
    } else if (entry->status == LOCK_OWNER) {
        owners = NULL;
        // not found in retired, need to make globally visible if rc = commit
        if (rc == RCOK && (entry->type == LOCK_EX))
            entry->access->orig_row->copy(entry->access->data);
    } else if (entry->status == LOCK_WAITER) {
#if DEBUG_BAMBOO 
		UInt32 cnt = 0;
		BBLockEntry * en = waiters_head;
		bool found = false;
		while(en) {
			if (en == entry) {
				found = true;
			}
			en = en->next;
			cnt++;
		}
		assert(found);
		assert(cnt == waiter_cnt);
		assert(cnt != 0);
        LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
		assert(waiter_cnt == cnt-1);
#else
        LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
#endif
    } else {
		// already removed
    }
    return_entry(entry);
    if (!owners) {
        bring_next(NULL);
    }
    assert(owners || retired_head || (waiter_cnt == 0));
    // WAIT - done releasing with is_abort = true
    // FINISH - done releasing with is_abort = false
#if PF_CS
    INC_STATS(entry->txn->get_thd_id(), time_release_cs, get_sys_clock() -
  starttime);
#endif
#if DEBUG_BAMBOO
	printf("[txn-%lu] lock_release(%p, %d) status=%d ts=%lu\n", entry->txn->get_txn_id(), this, entry->type, rc, entry->txn->get_ts());
#endif
    unlock(entry);
#if PF_ABORT 
    if (entry->txn->abort_chain > 0) {
    UPDATE_STATS(entry->txn->get_thd_id(), max_abort_length, entry->txn->abort_chain);
    INC_STATS(entry->txn->get_thd_id(), cascading_abort_times, 1);
    INC_STATS(entry->txn->get_thd_id(), abort_length, entry->txn->abort_chain);
  }
#endif
    return RCOK;
}


inline
bool Row_bamboo::bring_next(txn_man * txn) {
    bool has_txn = false;
    BBLockEntry * entry = waiters_head;
    BBLockEntry * next = NULL;
    bool retired_has_write = (retired_tail && (retired_tail->type == LOCK_EX || !retired_tail->is_cohead));
    // If any waiter can join the owners, just do it
    while (entry) {
		// XXX(zhihan): entry may not be waiters_head 
		next = entry->next;
        if (!owners || entry->type == LOCK_SH) {
            if (entry->type == LOCK_EX) { // !owners
                if (retired_has_write) {
                    // XXX(zhihan): decide if should read the dirty data
                    // it does not only benefit itself but also others.
                    // now everybody is waiting. should benefit times waiter cnt
#if BB_AUTORETIRE
                    int benefit = (benefit1 + benefit2) / (benefit_cnt1 +
                        benefit_cnt2);
                    int cost = entry->txn->get_exec_time();
                    bool read_dirty_data = ((benefit - cost) > 0);
#else
                    bool read_dirty_data = true;
#endif
                    if (read_dirty_data) {
                        // add to owners
                        owners = entry;
                        entry->status = LOCK_OWNER;
                        UPDATE_RETIRE_INFO(owners, retired_tail);
		    			BRING_OUT_WAITER(entry);
                    }
                } else {
                    // add to owners
                    owners = entry;
                    entry->status = LOCK_OWNER;
                    UPDATE_RETIRE_INFO(owners, retired_tail);
		    		BRING_OUT_WAITER(entry);
                }
                break; // owner is taken ~
            } else {
                // decide if should read dirty data
                if (retired_has_write) {
#if BB_AUTORETIRE
                    int benefit = (benefit1 + benefit2) / (benefit_cnt1 +
                        benefit_cnt2);
                    int cost = entry->txn->get_exec_time();
                    bool read_dirty_data = ((benefit - cost) > 0);
#else
                    bool read_dirty_data = true;
#endif
                    if (read_dirty_data) {
                        // add to retired
                        UPDATE_RETIRE_INFO(entry, retired_tail);
                        ADD_TO_RETIRED_TAIL(entry);
		    			BRING_OUT_WAITER(entry);
                    }
                } else {
                    // add to retired
                    UPDATE_RETIRE_INFO(entry, retired_tail);
                    ADD_TO_RETIRED_TAIL(entry);
		    		BRING_OUT_WAITER(entry);
                }
            }
			entry = next;
        } else
            break; // no promotable waiters
    }
    assert(owners || retired_head || (waiter_cnt == 0));
    return has_txn;
}

// return next lock entry in the retired list after removing en (and its
// descendants if is_abort = true)
inline
BBLockEntry * Row_bamboo::rm_from_retired(BBLockEntry * en, bool is_abort, txn_man * txn) {
    if (is_abort && (en->type == LOCK_EX)) {
        CHECK_ROLL_BACK(en); // roll back only for the first-conflicting-write
        en->txn->lock_abort = true;
        en = remove_descendants(en, txn);
        return en;
    } else {
        BBLockEntry * next = en->next;
        update_entry(en);
        LIST_RM(retired_head, retired_tail, en, retired_cnt);
        return_entry(en);
        return next;
    }
}

// used when entry is removed from retired, need to update other's co-head info
// case 1: no following nodes (!next && !owners)
//         return
// case 2.1: entry is RD
// case 2.1.1: [entry] is co-head
// - [R](R)W       W is also co-head, no changes needed
// case 2.1.2: [entry] is not co-head -> there is a W as co-head
// - W(R)[R](R)R  R will not become co-head
// - W(R)[R](R)W  W will not become co-head
// case 2.2: entry is WR
// [entry] must be co-head, otherwise rm_descendants & no need to call this
// rule: update util first write (include owners)
// - [W]RRWR      all RDs till first WR (included), e.g. RRW, new co-head
// - [W]WR        first WR becomes new co-head
inline
void Row_bamboo::update_entry(BBLockEntry * entry) {
    if (!entry->next && !owners) {
        return; // nothing to update
    }
    if (entry->type == LOCK_SH) {
        return;
    } else {
        assert(entry->is_cohead);
        entry = entry->next;
        bool first_write = false;
        while(entry && !first_write) {
            if (entry->type == LOCK_EX) {
                first_write = true;
            }
            assert(!entry->is_cohead);
            entry->is_cohead = true;
#if PF_CS
            uint64_t starttime = get_sys_clock();
            entry->txn->decrement_commit_barriers();
            INC_STATS(entry->txn->get_thd_id(), time_semaphore_cs,
                get_sys_clock() - starttime);
#else
            entry->txn->decrement_commit_barriers();
#endif
            if (entry->start_ts != 0)
                record_benefit(get_sys_clock() - entry->start_ts);
            entry = entry->next;
        }
        if (!first_write && owners) {
            entry = owners;
            entry->is_cohead = true;
#if PF_CS
            uint64_t starttime = get_sys_clock();
            entry->txn->decrement_commit_barriers();
            INC_STATS(entry->txn->get_thd_id(), time_semaphore_cs,
                get_sys_clock() - starttime);
#else
            entry->txn->decrement_commit_barriers();
#endif
            if (entry->start_ts != 0)
                record_benefit(get_sys_clock() - entry->start_ts);
        }
    }
}

inline
BBLockEntry * Row_bamboo::remove_descendants(BBLockEntry * en, txn_man *
txn) {
    // en->type must be LOCK_EX, which conflicts with everything after it.
    // including owners
    assert(en->type == LOCK_EX);
    BBLockEntry * prev = en->prev;
    BBLockEntry * to_return;
    // abort till end, no need to update barrier as set abort anyway
    LIST_RM_SINCE(retired_head, retired_tail, en);
    while(en) {
        en->txn->set_abort();
        to_return = en;
        retired_cnt--;
#if PF_ABORT 
        txn->abort_chain++;
#endif
        en = en->next;
        return_entry(to_return);
    }
    // empty owners
    if (owners) {
#if PF_ABORT 
        txn->abort_chain++;
#endif
        owners->txn->set_abort();
        return_entry(owners);
        owners = NULL;
    }
    assert(!retired_head || retired_head->is_cohead);

    if (prev)
        return prev->next;
    else
        return retired_head;
}

inline
RC Row_bamboo::insert_read_to_retired(BBLockEntry * to_insert, ts_t ts, 
				Access * access) {
	RC rc = RCOK;
	BBLockEntry * en = retired_head;
	for (UInt32 i = 0; i < retired_cnt; i++) {
		if ((en->type == LOCK_EX) && (en->txn->get_ts() > ts)) {
			break;
		}
		en = en->next;
	}
	if (en) {
		access->data->copy(en->access->orig_data);
		INSERT_TO_RETIRED(to_insert, en);
		rc = FINISH;
	} else {
		if (owners) {
			access->data->copy(owners->access->orig_data);
			INSERT_TO_RETIRED_TAIL(to_insert);
			rc = FINISH;
		} else {
			UPDATE_RETIRE_INFO(to_insert, retired_tail);
			ADD_TO_RETIRED_TAIL(to_insert);
			rc = RCOK;
		}
	}
	to_insert->txn->lock_ready = true;
#if DEBUG_BAMBOO
	// go through retired list and make sure
	// 1) the retired cnt is correct
	// 2) ordered by timestamp (allow disordered reads)
	// 3) the delta and cohead is correct
	en = retired_head;
	UInt32 cnt = 0;
	ts_t largest_ts = 0;
	ts_t largest_wr_ts = 0;	
	bool cohead = true;
	bool has_write = false;
	while(en) {
		if (en == retired_head) {
			assert(en->is_cohead);
		} else {
			// update expected cohead 
			if (has_write)
				cohead = false;
			else
				cohead = true;
			// check cohead
			assert(en->is_cohead == cohead);
		}
		// validate ts order
		ts_t ts = en->txn->get_ts();
		if (en->type == LOCK_EX) {
			assert(ts >= largest_ts);
			largest_wr_ts = ts;
			largest_ts = ts;
			has_write = true;
		} else {
			assert(ts > largest_wr_ts || en->is_cohead);
			largest_ts = max(ts, largest_ts);
		}
		cnt++;
		en = en->next;
	}
	assert(cnt == retired_cnt);
#endif
	return rc;
}
