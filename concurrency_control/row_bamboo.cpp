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
}

// taking the latch
void Row_bamboo::lock(txn_man * txn) {
    if (likely(g_thread_cnt > 1)) {
            if (unlikely(g_central_man))
                glob_manager->lock_row(_row);
            else {
#if LATCH == LH_SPINLOCK
               pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_lock( latch );
#else
                latch->acquire(txn->mcs_node);
#endif
            }
    }
};

// release the latch
void Row_bamboo::unlock(txn_man * txn) {
        if (likely(g_thread_cnt > 1)) {
            if (unlikely(g_central_man))
                glob_manager->release_row(_row);
            else {
#if LATCH == LH_SPINLOCK
                pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
                pthread_mutex_unlock( latch );
#else
                latch->release(txn->mcs_node);
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
#if PF_MODEL
    INC_STATS(txn->get_thd_id(), lock_acquire_cnt, 1);
#endif
#if PF_CS
    uint64_t starttime = get_sys_clock();
#endif
    // take the latch
    lock(txn);
    COMPILER_BARRIER
#if PF_CS
    uint64_t endtime = get_sys_clock();
    INC_STATS(txn->get_thd_id(), time_get_latch, endtime - starttime);
    starttime = endtime;
#endif
    // timestamp
    ts_t ts = 0;
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
			owner_ts = owners->txn->get_ts();
#if BB_DYNAMIC_TS
            // the only writer in owner may be unassigned
            owner_ts = assign_ts(owner_ts, owners->txn);
            // self may be unassigned
            ts = assign_ts(ts, txn);
#endif
            if (a_higher_than_b(owner_ts, ts)) { // owner has higher priority
                // add to waiters
				add_to_waiters(ts, to_insert);
				rc = WAIT;
                goto final; // since owner is blocking others
            } else { // ts has higher priority than owner
#if BB_OPT_RAW
                // if has any retired, retired must have ts since there's owner
				rc = insert_read_to_retired(to_insert, ts, access);
				goto final;
#else
                // wound and add to waiters (rc=WAIT)
                if (wound_retired_rd(ts, to_insert) == Abort) {
					rc = Abort;
					goto final;
				}
                // wound owners
                if (wound_owner(to_insert) == Abort) {
					rc = Abort;
					goto final;
				}
				add_to_waiters(ts, to_insert);		
                rc = WAIT;
#endif
            }
        } else { // no owners
#if PF_MODEL
            if (waiter_cnt == 0 && (!retired_tail || (retired_tail->is_cohead && retired_tail->type == LOCK_SH)))
                INC_STATS(txn->get_thd_id(), lock_directly_cnt, 1);
#endif
#if BB_DYNAMIC_TS
            // no owner, retired may not have ts.
            if (retired_tail) {
                if (retired_tail->is_cohead) {
                    // case 1: [W][][] / [W][][W]
                    if (retired_tail->type == LOCK_EX && !waiters_head) {
                        // assign retired head and assign self
                        assign_ts(0, retired_tail->txn);
                        ts = assign_ts(ts, txn);
                    } 
                    // else, case 2: [RRR][][] / [RRR][][W]
                    // all reads, then no need for ts. only case to have self=0
                } else {
                    // case 1: [RW] 
                    // case 3: [WR] 
                    // case 2: [WW] 
                    // retire list all assigned, need to assign self
                    ts = assign_ts(ts, txn);
                }
            }
#endif
#if BB_OPT_RAW
			// no owners, if waiters_head has higher priority, then wait. 
			if (waiters_head && a_higher_than_b(waiters_head->txn->get_ts(), ts)) {
				add_to_waiters(ts, to_insert);
				rc = WAIT;
			} else {
				// else, insert to retired list
				rc = insert_read_to_retired(to_insert, ts, access);
			}
			goto final;
#else
            // no owner, wound and add to retired (rc=RCOK)
            if (wound_retired_rd(ts, to_insert) == Abort) {
				rc = Abort;
				goto final;
			}
            UPDATE_RETIRE_INFO(to_insert, retired_tail);
            ADD_TO_RETIRED_TAIL(to_insert);
#if PF_CS
            INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif
            COMPILER_BARRIER
            unlock(txn);
            return rc;
            //goto final; // no owner -> no waiter, no need to promote
#endif
        }
    } else { // LOCK_EX
        // grab directly, no ts needed
        if (!retired_head && !owners) {
            owners = to_insert;
            owners->status = LOCK_OWNER;
            owners->txn->lock_ready = true;
            UPDATE_RETIRE_INFO(to_insert, retired_tail);
#if PF_CS
            INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif
            COMPILER_BARRIER
            unlock(txn);
#if PF_MODEL
            INC_STATS(txn->get_thd_id(), lock_directly_cnt, 1);
#endif
            return rc;
            // goto final;
        }
        // assign ts
        if (owners)
            owner_ts = owners->txn->get_ts();
#if BB_DYNAMIC_TS
        ts = txn->get_ts();
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
        } else {
			// self is assigned but txns in the list may not be assigned
			// three cases of where things are not assigned: 
			// case 1: [R][][], reads must be killed. 
			// case 2: [W][][], write must be killed
			// case 3: [][W][], write must be killed
			// must be handled by the follow that if unassigned must be killed
		}
#endif
        // wound retired
        if (wound_retired_wr(ts, to_insert) == Abort) {
              rc = Abort;
              goto final;
        }
        // wound owners
        if (owners && (owner_ts == 0 || a_higher_than_b(ts, owner_ts))) {
            if (wound_owner(to_insert) == Abort) {
                rc = Abort;
                goto final;
            }
        }
        // add self to waiters
        add_to_waiters(ts, to_insert);
        rc = WAIT;
#if DEBUG_TMP
        printf("[txn-%lu] lock_get(%p, %d) status=%d ts=%lu\n", txn->get_txn_id(), this, type, rc, ts);
#endif
    }
    // promote waiters, need to decide whether to read dirty data
    if (bring_next(txn)) {
        rc = RCOK;
    } else {
        goto final;
    }
final:
#if DEBUG_BAMBOO
	assert(((rc == WAIT) == (!txn->lock_ready)) || rc == Abort);
	check_correctness();
#endif
#if PF_CS
    INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif
    // release the latch
    COMPILER_BARRIER
    unlock(txn);
    if (rc == RCOK || rc == FINISH)
        txn->lock_ready = true;
    return rc;
}

RC Row_bamboo::lock_retire(BBLockEntry * entry) {
    ASSERT(entry->type == LOCK_EX);
#if PF_CS
    uint64_t starttime = get_sys_clock();
#endif
    lock(entry->txn);
    COMPILER_BARRIER
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
    COMPILER_BARRIER
    unlock(entry->txn);
    return rc;
}

RC Row_bamboo::lock_release(BBLockEntry * entry, RC rc) {
	if (entry->status == LOCK_DROPPED)
	    return RCOK;
#if PF_ABORT 
    entry->txn->abort_chain = 0;
#endif
#if PF_CS
    uint64_t starttime = get_sys_clock();
#endif
    lock(entry->txn);
    COMPILER_BARRIER
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
        if (rc == RCOK && (entry->type == LOCK_EX)) {
#if PF_CS
                uint64_t startt = get_sys_clock();
                entry->access->orig_row->copy(entry->access->data);
                INC_STATS(entry->txn->get_thd_id(), time_copy, get_sys_clock() - startt);
#else
                entry->access->orig_row->copy(entry->access->data);
#endif
        }
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
    COMPILER_BARRIER
    unlock(entry->txn);
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
#if DEBUG_BAMBOO
    check_correctness();
#endif
    bool has_txn = false;
    BBLockEntry * entry = waiters_head;
    BBLockEntry * next = NULL;
#if BB_AUTORETIRE
    bool retired_has_write = (retired_tail && (retired_tail->type == LOCK_EX || !retired_tail->is_cohead));
#endif
    // If any waiter can join the owners, just do it
    while (entry) {
		// XXX(zhihan): entry may not be waiters_head 
		next = entry->next;
        if (!owners) {
            if (entry->type == LOCK_EX) { // !owners
#if BB_AUTORETIRE
                printf("ERROR: not support AUTORETIRE yet\n");
#else
                // add to owners
                owners = entry;
                entry->status = LOCK_OWNER;
                UPDATE_RETIRE_INFO(owners, retired_tail);
                has_txn = bring_out_waiter(entry, txn);
#endif
                break;
            } else {
#if BB_AUTORETIRE && !BB_ALWAYS_RETIRED
                printf("ERROR: not support AUTORETIRE yet\n");
#else
                // add to retired
                UPDATE_RETIRE_INFO(entry, retired_tail);
                has_txn = bring_out_waiter(entry, txn);
                ADD_TO_RETIRED_TAIL(entry);
#endif
            }
			entry = next;
        } else
            break; // no promotable waiters
    }
    assert(owners || retired_head || (waiter_cnt == 0));
#if DEBUG_BAMBOO
    check_correctness();
#endif
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
        if (retired_cnt == 0)
          printf("error!\n");
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
// case 2.1.1.1: has other co-head
// - [R](R)W       W will not become co-head 
// case 2.1.1.2: no other co-head
// - [R]W          [acction point] W becomes cohead, decrement barrier
// case 2.1.2: [entry] is not co-head -> there is a W as co-head
// - W(R)[R](R)R  R will not become co-head
// - W(W)[R](R)R  R will not become co-head
// - W(R)[R](R)W  W will not become co-head
// - W(W)[R](R)W  W will not become co-head
// case 2.2: entry is WR
// [entry] must be co-head, otherwise rm_descendants & no need to call this
// rule: update util first write (include owners)
// - [W]RRWR      all RDs till first W (exclusive), e.g. RR, new co-head
// - [W]WR        first WR becomes new co-head
inline
void Row_bamboo::update_entry(BBLockEntry * entry) {
    if (!entry->next && !owners) {
        return; // nothing to update
    }
    if (entry->type == LOCK_SH) {
        // entry is the only RD cohead, update the write following it to be cohead
        if (entry->is_cohead && (!entry->prev)) {
            entry = entry->next;
            if (entry) {
                    if (entry->type == LOCK_SH)
                        return; // has other RD cohead
                    else {
#if PF_CS
                        DEC_BARRIER_PF(entry);
#else
                        DEC_BARRIER(entry);
#endif
                    }
            } else if (owners) {
#if PF_CS
                DEC_BARRIER_PF(owners);
#else
                DEC_BARRIER(owners);
#endif
            }
        }
        return;
    } else { // entry->type == LOCK_EX
        assert(entry->is_cohead);
        entry = entry->next;
        bool updated = false;
        while(entry) {
            if (entry->type == LOCK_EX) {
                if (!updated) {
                    updated = true;
#if PF_CS
                    DEC_BARRIER_PF(entry);
#else
                    DEC_BARRIER(entry);
#endif
                }
                break;
            } else {
                // update all RDs before 1st write
                updated = true;
#if PF_CS
                DEC_BARRIER_PF(entry);
#else
                DEC_BARRIER(entry);
#endif

            }
            entry = entry->next;
        }
        if (!updated && owners) {
#if PF_CS
                DEC_BARRIER_PF(owners);
#else
                DEC_BARRIER(owners);
#endif
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
        en->txn->set_abort(true);
        to_return = en;
        if (retired_cnt == 0)
          printf("error!\n");
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
    // TODO: handle case if en is committed. 
	for (UInt32 i = 0; i < retired_cnt; i++) {
		if ((en->type == LOCK_EX) && (en->txn->get_ts() > ts)) {
            // increment barrier anyway. if is not cohead, decrement the barrier
            en->txn->increment_commit_barriers();
            // compiler barrier
            COMPILER_BARRIER
            // check if status == committed, if still runnig, break. else continue
            if ((en->txn->commit_barriers & 3UL) ==  RUNNING) {
                if (!en->is_cohead)
                    en->txn->decrement_commit_barriers();
                else
                    en->is_cohead = false;
                assert((en->txn->commit_barriers & 3UL) != COMMITED);
			    break;
            }
		}
		en = en->next;
	}
	if (en) {
        assert(ts != 0);
        //INSERT_TO_RETIRED(to_insert, en);
        UPDATE_RETIRE_INFO(to_insert, en->prev); 
        LIST_INSERT_BEFORE_CH(retired_head, en, to_insert); 
        to_insert->status = LOCK_RETIRED; 
        retired_cnt++;
		to_insert->txn->lock_ready = true;
#if PF_CS
        uint64_t startt = get_sys_clock();
        access->data->copy(en->access->orig_data);
        INC_STATS(to_insert->txn->get_thd_id(), time_copy, get_sys_clock() - startt);
#else
        access->data->copy(en->access->orig_data);
#endif
		rc = FINISH;
#if DBEUG_BAMBOO
        check_correctness();
#endif
	} else {
		if (owners) {
            // insert before owners. 
            assert(ts != 0);
            owners->txn->increment_commit_barriers();
            COMPILER_BARRIER
            if ((owners->txn->commit_barriers & 3UL) == RUNNING) {
                if (!owners->is_cohead)
                    owners->txn->decrement_commit_barriers();
                else
                    owners->is_cohead = false;
                UPDATE_RETIRE_INFO(to_insert, retired_tail); 
                LIST_PUT_TAIL(retired_head, retired_tail, to_insert); 
                to_insert->status = LOCK_RETIRED; 
                retired_cnt++; 
#if PF_CS
                uint64_t startt = get_sys_clock();
                access->data->copy(owners->access->orig_data);
                INC_STATS(to_insert->txn->get_thd_id(), time_copy, get_sys_clock() - startt);
#else
                access->data->copy(owners->access->orig_data);
#endif
                to_insert->txn->lock_ready = true;
                rc = FINISH;
                assert((owners->txn->commit_barriers & 3UL) != COMMITED);
            } else {
                // COMMITED. add RD to waiters
		        add_to_waiters(ts, to_insert);
				rc = WAIT;
            }
		} else {
#if BB_AUTO_RETIRE
#if BB_ALWAYS_RETIRE_READ
		    if (waiters_head && a_high_than_b(waiters_head->txn->ts, ts)) {
		        add_to_waiters(ts, to_insert);
				rc = WAIT;
		    } else {
		        UPDATE_RETIRE_INFO(to_insert, retired_tail);
				ADD_TO_RETIRED_TAIL(to_insert);
				to_insert->txn->lock_ready = true;
				rc = RCOK;
		    }
#else
			// add to waiters
			bool retired_has_write = (retired_tail && (retired_tail->type == LOCK_EX || !retired_tail->is_cohead));
			if (retired_has_write) {
				add_to_waiters(ts, to_insert);
				rc = WAIT;
			} else {
				UPDATE_RETIRE_INFO(to_insert, retired_tail);
				ADD_TO_RETIRED_TAIL(to_insert);
				to_insert->txn->lock_ready = true;
				rc = RCOK;
			}
#endif
#else
            UPDATE_RETIRE_INFO(to_insert, retired_tail);
            ADD_TO_RETIRED_TAIL(to_insert);
            to_insert->txn->lock_ready = true;
            rc = RCOK;
#endif
		}
	}
#if DEBUG_TMP
    printf("[txn-%lu] lock_get(%p, RD) status=%d ts=%lu\n", to_insert->txn->get_txn_id(), this, rc, ts);
#endif
#if DBEUG_BAMBOO
    check_correctness();
#endif
	return rc;
}

#if DEBUG_BAMBOO
void Row_bamboo::check_correctness() {
    // go through retired list and make sure
	// 1) the retired cnt is correct
	// 2) ordered by timestamp (allow disordered reads)
	// 3) the cohead is correct
	UInt32 cnt = 0;
	ts_t largest_ts = 0;
	ts_t largest_wr_ts = 0;
	bool cohead = true;
	bool has_conflict = false;
	BBLockEntry * en = retired_head;
	while(en) {
		assert(en->status == LOCK_RETIRED);
		// validate ts order
		ts_t ts = en->txn->get_ts();
		if (en->type == LOCK_EX) {
			assert(ts >= largest_ts);
			largest_wr_ts = ts;
			largest_ts = ts;
			has_conflict = true;
		} else {
			//assert(ts > largest_wr_ts || en->is_cohead);
			largest_ts = max(ts, largest_ts);
		}
        // validate cohead
		if (en == retired_head) {
			assert(en->is_cohead);
		} else {
			// update expected cohead
			if (has_conflict) {
				cohead = false;
                assert(en->txn->commit_barriers != 0);
			} else {
				cohead = true;
            }
			// check cohead
			assert(en->is_cohead == cohead);
		}
		cnt++;
		en = en->next;
	}
	// check owner
	if (owners) {
		assert(owners->status == LOCK_OWNER);
	    assert(owners->is_cohead == (retired_head == NULL));
	    assert(owners->txn->get_ts() >= largest_ts);
		largest_ts = owners->txn->get_ts();
	}
	// check waiter
	cnt = 0;
	en = waiters_head;
	while (en) {
		assert(en->status == LOCK_WAITER);
	    assert(!en->is_cohead);
        if (en->type == LOCK_EX)
	        assert(en->txn->get_ts() >= largest_ts);
		largest_ts = en->txn->get_ts();
	    en = en->next;
		cnt++;
	}
	assert(cnt == waiter_cnt);
}
#endif
