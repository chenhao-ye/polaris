//
// Created by Zhihan Guo on 8/27/20.
//
#include "txn.h"
#include "row.h"
#include "row_ic3.h"

#if CC_ALG == IC3
void txn_man::begin_piece(int piece_id) {
  SC_PIECE * cedges = h_wl->get_cedges(curr_type, piece_id);
  if (cedges == NULL) {
    return; // skip to execute phase
  }
  int i;
  SC_piece * p_prime;
  for (i = 0; i < depqueue_sz; i++) { // for T' in T's depqueue
    p_prime = &(cedges[depqueue[i]->curr_type]);
    if (p_prime->txn_type != TPCC_ALL) {
      // exist c-edge with T'. wait for p' to commit
      while(p_prime <= depqueue[i]->curr_piece)
        continue;
    } else {
      // wait for T' to commit
      while(depqueue[i]->status == RUNNING)
        continue;
    }
  }
}

RC txn_man::end_piece(int piece_id) {
  // lock records in p’s read+writeset (using a sorted order)
  // validate p’s readset
  // foreach d in p.readset:
  // foreach d in p.writeset:
  // release grabbed locks
  curr_piece++;
  return RCOK;
}
RC
txn_man::validate_ic3() {
  return RCOK;
}

#endif
