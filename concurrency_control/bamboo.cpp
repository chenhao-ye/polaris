//
// Created by Zhihan Guo on 8/27/20.
//
#include "txn.h"
#include "row.h"
#include "row_bamboo.h"
//#include "row_bamboo_pt.h"

#if CC_ALG == BAMBOO
RC
txn_man::retire_row(int access_cnt){
  return accesses[access_cnt]->orig_row->retire_row(accesses[access_cnt]->lock_entry);
}
#endif

void
txn_man::decrement_commit_barriers() {
    //ATOM_SUB(*addr_barriers, 1UL << 2);
    ATOM_SUB(commit_barriers, 1UL << 2);
}

void
txn_man::increment_commit_barriers() {
    //ATOM_ADD(*addr_barriers, 1UL << 2);
    ATOM_ADD(commit_barriers, 1UL << 2);
}
