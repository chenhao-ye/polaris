//
// Created by Zhihan Guo on 8/27/20.
//
#include "txn.h"
#include "row.h"
#include "row_ic3.h"

#define APPEND_TO_DEPQ(T) { \
  if (depqueue[depqueue_sz] == NULL) \
    depqueue[depqueue_sz] = (TxnEntry *)_mm_malloc(sizeof(TxnEntry),64); \
  depqueue[depqueue_sz]->txn = T->txn; \
  depqueue[depqueue_sz]->txn_id = T->txn_id; \
  depqueue_sz++; \
}

#if CC_ALG == IC3
void txn_man::begin_piece(int piece_id) {
  access_marker = row_cnt;
  SC_PIECE * cedges = h_wl->get_cedges(curr_type, piece_id);
  if (cedges == NULL) {
    return; // skip to execute phase
  }
  int i;
  SC_piece * p_prime;
  for (i = 0; i < depqueue_sz; i++) { // for T' in T's depqueue
    p_prime = &(cedges[depqueue[i]->txn->curr_type]);
    if (p_prime->txn_type != TPCC_ALL) {
      // exist c-edge with T'. wait for p' to commit
      while(p_prime <= depqueue[i]->txn->curr_piece)
        continue;
    } else {
      // wait for T' to commit
      while(depqueue[i]->txn->status == RUNNING)
        continue;
    }
  }
}

RC txn_man::end_piece(int piece_id) {
  RC rc = RCOK;
  int piece_access_cnt = row_cnt - access_marker;
//  int write_set[piece_wr_cnt];
//  int cur_wr_idx = 0;
  int read_set[piece_access_cnt];
  int cur_rd_idx = 0;
  // lock records in p’s read+writeset (using a sorted order to avoid ddl)
  // read set is a superset of write set.
  for (int rid = access_marker; rid < row_cnt; rid ++) {
    read_set[cur_rd_idx ++] = rid;
  }
  // bubble sort the read_set, in primary key order
  for (int i = piece_access_cnt - piece_wr_cnt - 1; i >= 1; i--) {
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
  Access * access;
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]]
    row_t * row = access->orig_row;
    for (int j = 0; j < row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1 << j)) {
        if (!row->manager->try_lock(i) || (row->manager->get_tid(i) !=
        access->tids[i])) {
          rc = Abort;
          goto final;
        }
      }
    }
  }
  // foreach d in p.readset/p.writeset:
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]]
    row_t * row = access->orig_row;
    for (int j = 0; j < row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1 << j)) {
        LockEntry * Tw = row->manager->get_last_writer(i);
        APPEND_TO_DEPQ(Tw)
        if (access->wr_accesses & (1 << j)) {
          LockEntry * Trw = row->manager->get_last_accessor(i);
          APPEND_TO_DEPQ(Trw)
          row->manager->add_to_acclist(i, this, WR);
          //TODO: DB[d.key].stash = d.val
        } else {
          row->manager->add_to_acclist(i, this, RD);
        }
      }
    }
  }
  // release grabbed locks
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]]
    row_t * row = access->orig_row;
    for (int j = 0; j < row->get_field_cnt(); j++) {
      if (accesses[read_set[i]]->rd_accesses & (1 << j))
        row->manager->release(i);
    }
  }
  curr_piece++;

final:
  if (rc == Abort) {
    // reset access marker
    row_cnt = access_marker;
  }
  return rc;
}

RC
txn_man::validate_ic3() {
  // for T' in depqueue, wait till T' commit
  for (int i = 0; i < depqueue_sz; i++) {
    // TODO: what if committed and started next txn?
    while (depqueue[i]->txn->get_txn_id() == depqueue[i]->txn_id &&
    depqueue[i]->txn->status == RUNNING) {
      PAUSE
      continue;
    }
    if (depqueue[i]->txn->status == Abort) {
      return Abort;
    }
  }
  Access * access;
  for (int i = 0; i < row_cnt; i++) {
    access = accesses[i];
    for (int j = 0; j < access->orig_row->get_field_cnt(); j++) {
      if (access->wr_accesses & (1 << j)) {
        access->orig_row->set_value(j, access->data->get_value(j));
        access->orig_row->rm_from_acclist(j, this);
      }
    }
  }
  return RCOK;
}

#endif
