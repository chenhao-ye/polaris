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
    ts_t owner_ts;
    ts_t en_ts;
#if !DYNAMIC_TS
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
#if DYNAMIC_TS
            // the only writer in owner may be unassigned
            owner_ts = assign_ts(owner_ts, owners->txn);
            // self may be unassigned
            ts = assign_ts(ts, txn);
#endif
            if (owner_ts < ts) {
                // add to waiters
                ADD_TO_WAITERS(en, to_insert);
                goto final; // since owner is blocking others
            } else {
                // if has any retired, retired must have ts since there's owner
#if BB_RAW_OPT
                CHECK_AND_INSERT_RETIRED(en, to_insert);
#else
                // wound owners
                WOUND_OWNER(to_insert);
                // wound and add to waiters (rc=WAIT)
                WOUND_RETIRED(en, to_insert);
                ADD_TO_WAITERS(en, to_insert);
#endif
            }
        } else { // no owners
            assert(waiter_cnt == 0); // since owner is empty
#if DYNAMIC_TS
            // no owner, retired may not have ts.
            // if all read, then no need for ts. only case to have self=0
            // if any writes, then it has to be just one write in the retired
            // list, with no other reads; then may need to assign ts
            if (retired_tail && (retired_tail->type == LOCK_EX)) {
                assign_ts(0, retired_tail->txn);
                assign_ts(ts, txn);
            }
#endif
#if BB_RAW_OPT
            CHECK_AND_INSERT_RETIRED(en, to_insert);
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
#if DYNAMIC_TS
        // assign ts
        if (owners)
            owner_ts = owners->txn->get_ts();
        if (ts == 0) {
            if (owners) {
                if (!retired_head) {
                    // [ ][W][...]
                    assign_ts(owner_ts, owners->txn);
                    assign_ts(ts, txn);
                } else {
                    // [...][W], everybody is assigned
                    assign_ts(ts, txn);
                }
            } else {
                if (retired_head) {
                    // [RR/W][][]
                    en = retired_head;
                    for (int i = 0; i < retired_cnt; i++) {
                        en_ts = en->txn->get_ts();
                        assign_ts(en_ts, en->txn);
                        en = en->next;
                    }
                    assign_ts(ts, txn);
                } // else [][][] -> no need to assign self
            }
        }
#endif
        // wound retired
        en = retired_head;
        for (int i = 0; i < retired_cnt; i++) {
            if (en->txn->get_ts() < ts) {
                TRY_WOUND(en, to_insert);
                en = rm_from_retired(en, true, txn);
            } else
                en = en->next;
        }
        // wound owners
        if (owners && owner_ts < ts)
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
    // release the latch
    unlock(to_insert);
    if (rc == RCOK || rc == FINISH)
        txn->lock_ready = true;
    return rc;
}

RC Row_bamboo_pt::lock_retire(void * addr) {
    BBLockEntry * entry = (BBLockEntry *) addr;
    ASSERT(entry->type == LOCK_EX);
#if DEBUG_CS_PROFILING
    uint64_t starttime = get_sys_clock();
#endif
    lock(entry);
#if DEBUG_CS_PROFILING
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
#if DEBUG_CS_PROFILING
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

#if DEBUG_CS_PROFILING
    INC_STATS(entry->txn->get_thd_id(), time_retire_cs, get_sys_clock() -
    starttime);
#endif
    unlock(entry);
    return rc;
}

RC Row_bamboo_pt::lock_release(void * addr, RC rc) {
    BBLockEntry * entry = (BBLockEntry *) addr;
#if DEBUG_ABORT_LENGTH
    entry->txn->abort_chain = 0;
#endif
#if DEBUG_CS_PROFILING
    uint64_t starttime = get_sys_clock();
#endif
    lock(entry);
#if DEBUG_CS_PROFILING
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
        LIST_RM(waiters_head, waiters_tail, entry, waiter_cnt);
    } else {
    }
    return_entry(entry);
    if (!owners) {
        bring_next(NULL);
    }
    // WAIT - done releasing with is_abort = true
    // FINISH - done releasing with is_abort = false
#if DEBUG_CS_PROFILING
    INC_STATS(entry->txn->get_thd_id(), time_release_cs, get_sys_clock() -
  starttime);
#endif
    unlock(entry);
#if DEBUG_ABORT_LENGTH
    if (entry->txn->abort_chain > 0) {
    UPDATE_STATS(entry->txn->get_thd_id(), max_abort_length, entry->txn->abort_chain);
    INC_STATS(entry->txn->get_thd_id(), cascading_abort_times, 1);
    INC_STATS(entry->txn->get_thd_id(), abort_length, entry->txn->abort_chain);
  }
#endif
    return RCOK;
}


inline
bool Row_bamboo_pt::bring_next(txn_man * txn) {
    bool has_txn = false;
    BBLockEntry * entry;
    bool retired_has_write = (retired_tail && (retired_tail->type == LOCK_EX ||
        !retired_tail->is_cohead));
    // If any waiter can join the owners, just do it!
    while (waiters_head) {
        if (!owners || (!conflict_lock(owners->type, waiters_head->type))) {
            LIST_GET_HEAD(waiters_head, waiters_tail, entry);
            waiter_cnt --;
            if (entry->type == LOCK_EX) {
                if (retired_has_write) {
                    // TODO: decide if should read the dirty data
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
                    }
                } else {
                    // add to owners
                    owners = entry;
                    entry->status = LOCK_OWNER;
                    UPDATE_RETIRE_INFO(owners, retired_tail);
                }
                break; // owner is taken ~
            } else {
                // decide if should read dirty data
                if (retired_has_write) {
#if BB_AUTORETIRE
                    // TODO: decide if should read the dirty data
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
                    }
                } else {
                    // add to retired
                    UPDATE_RETIRE_INFO(entry, retired_tail);
                    ADD_TO_RETIRED_TAIL(entry);
                }
            }
            entry->txn->lock_ready = true;
            if (txn == entry->txn) {
                has_txn = true;
            }
        } else
            break;
    }
    return has_txn;
}

// return next lock entry in the retired list after removing en (and its
// descendants if is_abort = true)
inline
BBLockEntry * Row_bamboo_pt::rm_from_retired(BBLockEntry * en, bool is_abort, txn_man * txn) {
    if (is_abort && (en->type == LOCK_EX)) {
        CHECK_ROLL_BACK(en); // roll back only for the first-conflicting-write
        en->txn->lock_abort = true;
        en = remove_descendants(en, txn);
        return en;
    } else {
        BBLockEntry * next = en->next;
        // TODO: also needs to update entry info in owners if the last
        update_entry(en);
        LIST_RM(retired_head, retired_tail, en, retired_cnt);
        return_entry(en);
        return next;
    }
}

inline
void Row_bamboo_pt::update_entry(BBLockEntry * entry) {
    if (!entry->next && !owners) {
        return; // nothing to update
    }
    if (entry->type == LOCK_SH) {
        // cohead: no need to update
        // delta: update next entry only
        if (entry->prev) {
            if (entry->next)
                entry->next->delta = entry->prev->type == LOCK_EX;
            else
                owners->delta = entry->prev->type == LOCK_EX;
        }
        return;
    }
    // if entry->type == LOCK_EX, has to be co-head and txn commits, otherwise
    // abort will not call this
    // TODO: update owners' commit barrier if needed (just bring in owners in
    //  the loop)
    ASSERT(entry->is_cohead);
    BBLockEntry * en = entry->next;
    bool checked_owners = false;
    if (!en) {
        en = owners;
        checked_owners = true;
    }
    if (entry->prev) {
        // prev can only be reads
        // cohead: whatever it is, it becomes NEW cohead and decrement barrier
        // delta: set only the immediate next
        en->delta = false;
        // R(W-committed)RRR -- yes for entry->next->next, i.e. en->next
        // R(W-committed)RWR -- yes
        // R(W-committed)WRR -- no
        // R(W-committed)WWR -- no
        while (en && !(en->delta)) {
            en->is_cohead = true;
#if DEBUG_CS_PROFILING
            uint64_t starttime = get_sys_clock();
            en->txn->decrement_commit_barriers();
            record_benefit(get_sys_clock() - en->txn->start_ts);
            INC_STATS(en->txn->get_thd_id(), time_semaphore_cs, get_sys_clock() - starttime);
#else
            en->txn->decrement_commit_barriers();
            record_benefit(get_sys_clock() - en->txn->start_ts);
#endif
            en = en->next;
            if (!en && !checked_owners) {
                checked_owners = true;
                en = owners;
            }
        }
    } else {
        // cohead: whatever it is becomes cohead
        // delta: whatever it is delta is false
        entry->delta = false;
        do { // RRRRW
            en->is_cohead = true;
#if DEBUG_CS_PROFILING
            uint64_t starttime = get_sys_clock();
            en->txn->decrement_commit_barriers();
            record_benefit(get_sys_clock() - en->txn->start_ts);
            INC_STATS(en->txn->get_thd_id(), time_semaphore_cs, get_sys_clock() - starttime);
#else
            en->txn->decrement_commit_barriers();
            record_benefit(get_sys_clock() - en->txn->start_ts);
#endif
            en = en->next;
            if (!en && !checked_owners) {
                checked_owners = true;
                en = owners;
            }
        } while(en && !(en->delta));
    }
}

inline
BBLockEntry * Row_bamboo_pt::remove_descendants(BBLockEntry * en, txn_man *
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
#if DEBUG_ABORT_LENGTH
        txn->abort_chain++;
#endif
        en = en->next;
        return_entry(to_return);
    }
    // empty owners
    if (owners) {
#if DEBUG_ABORT_LENGTH
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


