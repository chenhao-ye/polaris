#include "row.h"
#include "txn.h"
#include "row_bamboo.h"
#include "mem_alloc.h"
#include "manager.h"

void Row_bamboo::init(row_t * row) {
  Row_bamboo_pt::init(row);
  // local timestamp
  local_ts = -1;
  txn_ts = 0;
}

RC Row_bamboo::lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int
&txncnt, Access * access) {
  // allocate an lock entry
  ASSERT(CC_ALG == BAMBOO);
  BBLockEntry * to_insert = get_entry(access);
  to_insert->txn = txn;
  to_insert->type = type;
  BBLockEntry * en = NULL;

#if DEBUG_CS_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  lock(to_insert);
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), time_get_latch, get_sys_clock() - starttime);
  starttime = get_sys_clock();
#endif

  // 1. set txn to abort in owners and retired
  RC rc = WAIT;
  RC status = RCOK;
  // if unassigned, grab or assign the largest possible number
  local_ts = -1;
  ts_t ts = txn->get_ts(); // ts: orig timestamp of txn
  txn_ts = ts; // txn_ts: current timestamp of txn
  if (ts == 0) {
    // test if can grab the lock without assigning priority
    if ((waiter_cnt == 0) &&
        (retired_cnt == 0 || (!conflict_lock(retired_tail->type, type) && retired_tail->is_cohead)) &&
        (owner_cnt == 0 || !conflict_lock(owners->type, type)) ) {
      // add to owners directly
      txn->lock_ready = true;
      LIST_PUT_TAIL(owners, owners_tail, to_insert);
      to_insert->status = LOCK_OWNER;
      owner_cnt++;
      rc = RCOK;
      goto final;
    }
    // else has to assign a priority and add to waiters first
    // assert(retired_cnt + owner_cnt != 0);
    // heuristic to batch assign ts:
    //int batch_n_ts = retired_cnt + owner_cnt + 1;

    int batch_n_ts = 1;
    if ( waiter_cnt == 0 ) {
      if (retired_tail && (retired_tail->txn->get_ts() == 0)) {
        batch_n_ts += retired_cnt;
      }
      batch_n_ts += owner_cnt;
    }
    //local_ts = txn->set_next_ts(retired_cnt + owner_cnt + 1);
    local_ts = txn->set_next_ts(batch_n_ts);
    if (local_ts != 0) {
      // if != 0, already booked n ts.
      txn_ts = local_ts;
      local_ts = local_ts - batch_n_ts + 1;
      //assert(txn->get_ts() != local_ts); // make sure did not change pointed addr
    } else {
      // if == 0, fail to assign, oops, self has an assigned number anyway
      ts = txn->get_ts();
      txn_ts = ts;
    }
  }

  // 2. wound conflicts
  // 2.1 check retired
  fcw = NULL;
  status = wound_conflict(type, txn, ts==0, true, status);
  if (status == Abort) {
    rc = Abort;
    if (owner_cnt == 0)
      bring_next(NULL);
    return_entry(to_insert);
    goto final;
  }
#if BB_OPT_RAW
  else if (status == FINISH) {
    // RAW conflict, need to read its orig_data by making a read copy
    access->data->copy(fcw->access->orig_data);
    // insert before writer
    UPDATE_RETIRE_INFO(to_insert, fcw->prev);
    RECHECK_RETIRE_INFO(fcw, to_insert);
    LIST_INSERT_BEFORE_CH(retired_head, fcw, to_insert);
    to_insert->status = LOCK_RETIRED;
    retired_cnt++;
    fcw = NULL;
    txn->lock_ready = true;
    rc = FINISH;
    goto final;
  }
#endif

  // 2.2 check owners
  status = wound_conflict(type, txn, ts==0, false, status);
  if (status == Abort) {
    rc = Abort;
    if (owner_cnt == 0)
      bring_next(NULL);
    return_entry(to_insert);
    goto final;
  }
#if BB_OPT_RAW
  else if (status == FINISH) {
    // RAW conflict, need to read its orig_data by making a read copy
    access->data->copy(fcw->access->orig_data);
    // append to the end of retired
    UPDATE_RETIRE_INFO(to_insert, retired_tail);
    LIST_PUT_TAIL(retired_head, retired_tail, to_insert);
    to_insert->status = LOCK_RETIRED;
    retired_cnt++;
    fcw = NULL;
    txn->lock_ready = true;
    rc = FINISH;
    goto final;
  }
#endif

  // 2. insert into waiters and bring in next waiter
  to_insert->txn = txn;
  to_insert->type = type;
  en = waiters_head;
  while (en != NULL) {
    if (txn_ts < en->txn->get_ts())
      break;
    en = en->next;
  }
  if (en) {
    LIST_INSERT_BEFORE_CH(waiters_head, en, to_insert);
  } else {
    LIST_PUT_TAIL(waiters_head, waiters_tail, to_insert);
  }
  to_insert->status = LOCK_WAITER;
  waiter_cnt ++;
  txn->lock_ready = false;

  // 3. bring next available to owner in case both are read
  if (bring_next(txn)) {
    rc = RCOK;
  }

  // 4. retire read directly
#if RETIRE_ON
  if (owners && (waiter_cnt > 0) && (owners->type == LOCK_SH)) {
    // if retire turned on and share lock is the owner
    // move to retired
    BBLockEntry * to_retire = NULL;
    while (owners) {
      to_retire = owners;
      RETIRE_ENTRY(to_retire);
    }
    if (owner_cnt == 0 && bring_next(txn)) {
      rc = RCOK;
    }
  }
#endif
  final:
#if DEBUG_CS_PROFILING
  INC_STATS(txn->get_thd_id(), time_get_cs, get_sys_clock() - starttime);
#endif
  unlock(to_insert);
  return rc;
}

inline
RC Row_bamboo::wound_conflict(lock_t type, txn_man * txn, bool unassigned,
bool check_retired, RC status) {
  BBLockEntry * en;
  if (check_retired)
    en = retired_head;
  else
    en = owners;
  // flag for rechecking an entry when fail to assign a ts to get the updated ts
  bool recheck = false;
  // go through retire list
  while (en) {
    ts_t en_ts = en->txn->get_ts(); // fetch ts ahead of time
    if (unassigned) {
      if (en_ts == 0) {
        // both unassigned, can assign en_ts a smaller timestamp than txn's
        if (!en->txn->atomic_set_ts(local_ts)) { // it has a ts already
          recheck = true;
        } else {
          // assign a smaller timestamp booked ahead of time
          local_ts++;
        }
        if (!recheck)
          en = en->next;
      } else {
        // txn unassigned, en assigned
        TRY_WOUND_CONFLICT(en, en_ts, txn_ts, check_retired);
      }
    } else {
      if (en_ts == 0) {
        // txn assigned, en unassigned, abort en
        TRY_WOUND(en, check_retired);
      } else {
        // txn assigned, en assigned, check for conflicts
        TRY_WOUND_CONFLICT(en, en_ts, txn_ts, check_retired);
      }
    }
  }
  return RCOK;
}

