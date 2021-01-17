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
int
txn_man::get_txn_pieces(int tpe) {
  switch(tpe) {
    case TPCC_PAYMENT :
      return IC3_TPCC_PAYMENT_PIECES;
    case TPCC_NEW_ORDER:
      return IC3_TPCC_NEW_ORDER_PIECES;
    case TPCC_DELIVERY:
      return IC3_TPCC_DELIVERY_PIECES;
    default:
      assert(false);
  }
  return 0;
}

void txn_man::begin_piece(int piece_id) {
  //printf("begin piece %d\n", piece_id);
  piece_starttime = get_sys_clock();
  curr_piece = piece_id;
  access_marker = row_cnt;
#if !IC3_EAGER_EXEC
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
      while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() &&
          (p_prime->piece_id >= depqueue[i]->txn->curr_piece) &&
          (depqueue[i]->txn->status == RUNNING))
        continue;
    } else {
#if IC3_RENDEZVOUS
      // find the next avaiable rendezvous piece in T'
      if (piece_id + 1 != get_txn_pieces(curr_type)) {
        bool rendezvous = false;
        SC_PIECE * r;
        for (int q = piece_id+1; q < get_txn_pieces(curr_type); q++) {
          SC_PIECE * next_cedges = h_wl->get_cedges(curr_type, q);
	  if (next_cedges == NULL)
		  continue; // no conflicting edges
          r = &(next_cedges[depqueue[i]->txn->curr_type]);
          if (r->txn_type != TPCC_ALL) {
            rendezvous = true;
            break;
          }
        }
        if (rendezvous) {
          // exist rendezvous piece, only need to wait till r commit
          while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() &&
          (r->piece_id >= depqueue[i]->txn->curr_piece) &&
          (depqueue[i]->txn->status == RUNNING))
            continue;
          continue; // no need wait for T' to commit
        }
      }
#endif
      // wait for T' to commit
      starttime = get_sys_clock();
      while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() && (depqueue[i]->txn->status == RUNNING))
        continue;
    }
  }
  INC_TMP_STATS(get_thd_id(), time_wait, get_sys_clock() - starttime);
#endif
}

RC txn_man::end_piece(int piece_id) {
#if IC3_EAGER_EXEC
  SC_PIECE * cedges = h_wl->get_cedges(curr_type, piece_id);
  if (cedges != NULL) {
  int i;
  SC_PIECE * p_prime;
  uint64_t starttime = get_sys_clock();
  for (i = 0; i < depqueue_sz; i++) { // for T' in T's depqueue
    p_prime = &(cedges[depqueue[i]->txn->curr_type]);
    if (p_prime->txn_type != TPCC_ALL) {
      // exist c-edge with T'. wait for p' to commit
      while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() &&
      (p_prime->piece_id >= depqueue[i]->txn->curr_piece) &&
      (depqueue[i]->txn->status == RUNNING))
        continue;
    } else {
#if IC3_RENDEZVOUS
      // find the next avaiable rendezvous piece in T'
      if (piece_id + 1 != get_txn_pieces(curr_type)) {
        bool rendezvous = false;
        SC_PIECE * r;
        for (int q = piece_id+1; q < get_txn_pieces(curr_type); q++) {
          SC_PIECE * next_cedges = h_wl->get_cedges(curr_type, q);
	  if (next_cedges == NULL)
		  continue; // no conflicting edges
          r = &(next_cedges[depqueue[i]->txn->curr_type]);
          if (r->txn_type != TPCC_ALL) {
            rendezvous = true;
            break;
          }
        }
        if (rendezvous) {
          // exist rendezvous piece, only need to wait till r commit
          while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() &&
          (r->piece_id >= depqueue[i]->txn->curr_piece) &&
          (depqueue[i]->txn->status == RUNNING))
            continue;
          continue; // no need to wait till T' to commit
        }
      }
#endif
      // wait for T' to commit
      starttime = get_sys_clock();
      while(depqueue[i]->txn_id == depqueue[i]->txn->get_txn_id() && (depqueue[i]->txn->status == RUNNING))
        continue;
    }
  }
  INC_TMP_STATS(get_thd_id(), time_wait, get_sys_clock() - starttime);
  } // if (cedges != NULL), skip to validate phase.
  else
	  return RCOK;
#else
  SC_PIECE * cedges = h_wl->get_cedges(curr_type, piece_id);
  if (cedges == NULL) {
    return RCOK; // skip commit phase
  }
#endif
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
  // lock record's read+write set data, as write is the subset, just lock
  // read set; validate p’s readset
  int num_locked = 0;
  bool acquired = false;
  bool appended = false;
  Access * access;
  row_t * row;
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]];
    //assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
    row = access->orig_row;
#if !IC3_FIELD_LOCKING
    acquired = row->manager->try_lock();
    if (acquired) {
      num_locked++;
      access->lk_accesses = 1;
    }
    if (!acquired || (row->manager->get_tid() != access->tid)) {
      rc = Abort;
      goto final;
    }
#else
    for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1UL << j)) {
        acquired = row->manager->try_lock(j);
        if (acquired) {
          num_locked++;
          access->lk_accesses = (access->lk_accesses | (1UL << j));
        }
        if (!acquired || (row->manager->get_tid(j) != access->tids[j])) {
          rc = Abort;
          goto final;
        }
      }
    }
#endif
  }

  // foreach d in p.readset/p.writeset:
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]];
    //assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
    row = access->orig_row;
#if IC3_FIELD_LOCKING
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
#else
    IC3LockEntry * Tw = row->manager->get_last_writer();
    APPEND_TO_DEPQ(Tw);
    if (access->type == WR) {
      IC3LockEntry * Trw = row->manager->get_last_accessor();
      if (Trw != Tw) {
        APPEND_TO_DEPQ(Trw);
      }
      row->manager->add_to_acclist(this, WR);
    } else {
      row->manager->add_to_acclist(this, RD);
    }
#endif
  }

  // release grabbed locks
  for (int i = 0; i < piece_access_cnt; i++) {
    access = accesses[read_set[i]];
    //assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
    row = access->orig_row;
#if IC3_FIELD_LOCKING
    for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
      if (access->lk_accesses & (1UL << j)) {
        row->manager->release(j);
        num_locked--;
      }
    }
#else
    if (access->lk_accesses) {
      row->manager->release();
      num_locked--;
    }
#endif
  }
  assert(num_locked == 0);
  curr_piece++;

  final:
  if (rc == Abort) {
    // unlock locked entries
    for (int i = 0; i < piece_access_cnt; i++) {
      access = accesses[read_set[i]];
      //assert(read_set[i] < row_cnt && (read_set[i] >= access_marker));
      row = access->orig_row;
#if IC3_FIELD_LOCKING
      for (UInt32 j = 0; j < row->get_field_cnt(); j++) {
        if (access->lk_accesses & (1UL << j)) {
          row->manager->release(j);
          num_locked--;
        }
      }
#else
      if (access->lk_accesses) {
        row->manager->release();
        num_locked--;
      }
#endif
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
#if PF_BASIC 
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
#if PF_BASIC 
  INC_STATS(get_thd_id(), time_commit, get_sys_clock() - starttime);
#endif
  Access * access;
  for (int i = 0; i < row_cnt; i++) {
    access = accesses[i];
#if IC3_FIELD_LOCKING
    for (UInt32 j = 0; j < access->orig_row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1 << j)) {
        if (access->wr_accesses & (1 << j)) {
          access->orig_row->manager->update_version(j, this->get_txn_id());
          access->orig_row->set_value_plain(j, access->data->get_value_plain(j));
        }
        access->orig_row->manager->rm_from_acclist(j, this);
      }
    }
#else
      if (access->type == WR) {
        access->orig_row->manager->update_version(this->get_txn_id());
        // XXX(zhihan): copy modified fields only
        for (UInt32 j = 0; j < access->orig_row->get_field_cnt(); j++) {
          if (access->wr_accesses & (1 << j))
            access->orig_row->set_value_plain(j, access->data->get_value_plain(j));
        }
      }
    access->orig_row->manager->rm_from_acclist(this);
#endif
#if COMMUTATIVE_OPS
      // commutative ops
      if (access->com_op != COM_NONE ) {
#if IC3_FIELD_LOCKING
        access->orig_row->manager->update_version(access->com_col, get_txn_id());
#else
        if (access->type == RD)
          access->orig_row->manager->update_version(get_txn_id());
#endif
        if (access->com_op == COM_INC)
          access->orig_row->inc_value(access->com_col, access->com_val);
        else
          access->orig_row->dec_value(access->com_col, access->com_val);
        access->com_op = COM_NONE;
      }
#endif
  }
  return RCOK;
}

void
txn_man::abort_ic3() {
  // cascading aborts
  // 1. abort bit for each entry in the accessor list
  // 2. when aborts, unlinks its own entry from the accessor list that it has
  // updated and sets the abort bit for all entries appearing after itself.
  // 3. before commits, check the abort bits of all accessor list entries.
  // abort itself if has any abort bit set.
  // XXX(zhihan): to simplify the process and reduce overheads, set one
  // condition bit for each txn (status) and only check the status with atomic
  // operation while committing.

  // go through all accesses: rm self from acclist and set followers to abort
  Access * access;
  for (int i = 0; i < row_cnt; i++) {
    access = accesses[i];
#if IC3_FIELD_LOCKING
    for (UInt32 j = 0; j < access->orig_row->get_field_cnt(); j++) {
      if (access->rd_accesses & (1 << j)) {
        access->orig_row->manager->rm_from_acclist(j, this, true); // aborted
      }
    }
#else
    access->orig_row->manager->rm_from_acclist(this, true);
#endif
  }

}
#endif
