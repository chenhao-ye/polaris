//
// Created by Zhihan Guo on 8/27/20.
//
#include "txn.h"
#include "row.h"
#include "row_ic3.h"
#include "tpcc.h"

#define APPEND_TO_DEPQ(T) { \
  appended = false; \
  if (T != NULL) { \
  for (int k = 0; k < depqueue_sz; k++) { \
	  if (depqueue[k]->txn == T->txn) { \
		  appended = true; \
		  break; \
	  } \
  } \
  if (!appended) { \
    assert(depqueue_sz < THREAD_CNT); \
    if (depqueue[depqueue_sz] == NULL) \
      depqueue[depqueue_sz] = (TxnEntry *)_mm_malloc(sizeof(TxnEntry),64); \
    depqueue[depqueue_sz]->txn = T->txn; \
    depqueue[depqueue_sz]->txn_id = T->txn_id; \
    depqueue_sz++; \
  } \
  } \
}

#if CC_ALG == IC3
void txn_man::begin_piece(int piece_id) {
  //printf("begin piece %d\n", piece_id);
  piece_starttime = get_sys_clock();
  curr_piece = piece_id;
  access_marker = row_cnt;
  SC_PIECE * cedges = h_wl->get_cedges(curr_type, piece_id);
  if (cedges == NULL) {
    return; // skip to execute phase
  }
  int i;
  SC_PIECE * p_prime;
  uint64_t starttime = get_sys_clock();
  for (i = 0; i < depqueue_sz; i++) { // for T' in T's depqueue
    p_prime = &(cedges[depqueue[i]->txn->curr_type]);
    if (p_prime->txn_type != TPCC_ALL) {
      // exist c-edge with T'. wait for p' to commit
      while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() && ( p_prime->piece_id >=  depqueue[i]->txn->curr_piece))
        continue;
    } else {
      // wait for T' to commit
      starttime = get_sys_clock();
      while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() && (depqueue[i]->txn->status == RUNNING))
        continue;
    }
  }
  INC_TMP_STATS(get_thd_id(), time_wait, get_sys_clock() - starttime);
}

RC txn_man::end_piece(int piece_id) {
  RC rc = RCOK;
  int piece_access_cnt = row_cnt - access_marker;
  if (piece_access_cnt == 0)
	  return rc;
  int read_set[piece_access_cnt];
  int cur_rd_idx = 0;
  // lock records in p’s read+writeset (using a sorted order to avoid ddl)
  // read set is a superset of write set.
  for (int rid = access_marker; rid < row_cnt; rid ++) {
    read_set[cur_rd_idx ++] = rid;
    accesses[rid]->lk_accesses = 0;
  }
  // bubble sort the read_set, in primary key order
  for (int i = piece_access_cnt - 1; i >= 1; i--) {
    for (int j = 0; j < i; j++) {
      if (accesses[ read_set[j] ]->orig_row->get_primary_key() >
          accesses[ read_set[j + 1] ]->orig_row->get_primary_key())
      {
        int tmp = read_set[j];
        read_set[j] = read_set[j+1];
        read_set[j+1] = tmp;
      }
    }
  }
  // lock record's read+write set cell, as write is the subset, just lock
  // read set; validate p’s readset
  int num_locked = 0;
  bool acquired = false;
  bool appended = false;
  Access * access;
  row_t * row;
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]];
    assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
    row = access->orig_row;
    for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1UL << j)) {
	acquired = row->manager->try_lock(j);
	if (acquired) {
            num_locked++;
	    access->lk_accesses = (access->lk_accesses | (1UL << j));
	}
        if (!acquired || (row->manager->get_tid(j) != access->tids[j])) {
/*
          if (acquired)
          printf("[version fail] txn-%lu[i=%d] aborted piece %d at %p-%u\n", get_txn_id(), i, curr_piece, row, j);
	  else
          printf("[lock fail] txn-%lu[i=%d] aborted piece %d at %p-%u\n", get_txn_id(), i, curr_piece, row, j);
	  assert(false);
*/
          rc = Abort;
          goto final;
        }
      }
    }
  }
  // foreach d in p.readset/p.writeset:
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]];
    assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
    row = access->orig_row;
    for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1UL << j)) {
        IC3LockEntry * Tw = row->manager->get_last_writer(j);
        APPEND_TO_DEPQ(Tw);
        if (access->wr_accesses & (1UL << j)) {
          IC3LockEntry * Trw = row->manager->get_last_accessor(j);
          APPEND_TO_DEPQ(Trw);
          row->manager->add_to_acclist(j, this, WR);
          //TODO: DB[d.key].stash = d.val
        } else {
          row->manager->add_to_acclist(j, this, RD);
        }
      }
    }
  }
  // release grabbed locks
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]];
    assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
    row = access->orig_row;
    for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
      if (access->lk_accesses & (1UL << j)) {
        row->manager->release(j);
	num_locked--;
      }
    }
  }
  assert(num_locked == 0);
  curr_piece++;

final:
  if (rc == Abort) {
    // unlock locked entries
    for (int i = 0; i < piece_access_cnt; i++) {
      access = accesses[read_set[i]];
      assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
      row = access->orig_row;
      for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
        if (access->lk_accesses & (1 << j)) {
          row->manager->release(j);
          num_locked--;
        }
      }
      INC_STATS(get_thd_id(), time_abort, get_sys_clock() - piece_starttime);
    }
    assert(num_locked == 0);
    // reset access marker
    row_cnt = access_marker;
    //assert(false);
  }
  return rc;
}

RC
txn_man::validate_ic3() {
  // for T' in depqueue, wait till T' commit
#if DEBUG_PROFILING
  uint64_t starttime = get_sys_clock();
#endif
  for (int i = 0; i < depqueue_sz; i++) {
    while (depqueue[i]->txn->get_txn_id() == depqueue[i]->txn_id &&
    depqueue[i]->txn->status == RUNNING) {
      PAUSE
      continue;
    }
    if (depqueue[i]->txn->status == ABORTED) {
      return Abort;
    }
  }
#if DEBUG_PROFILING
    INC_STATS(get_thd_id(), time_commit, get_sys_clock() - starttime);
#endif
  Access * access;
  for (int i = 0; i < row_cnt; i++) {
    access = accesses[i];
    for (UInt32 j = 0; j < access->orig_row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1 << j)) {
        if (access->wr_accesses & (1 << j)) {
          access->orig_row->manager->update_version(j, this->get_txn_id());
	  //printf("txn-%lu update verison to its id for %p-%u\n", get_txn_id(), access->orig_row, j);
          access->orig_row->set_value_plain(j, access->data->get_value_plain(j));
        }
      access->orig_row->manager->rm_from_acclist(j, this);
      }
    }
    // debugging
    assert(access->data->data);
  }
  return RCOK;
}

void
txn_man::abort_ic3() {
  
}
#endif
