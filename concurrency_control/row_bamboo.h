#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

#include "row_bamboo_pt.h"

class Row_bamboo : public Row_bamboo_pt {
 public:
  void init(row_t * row) override;
  RC lock_get(lock_t type, txn_man * txn, Access * access) override {
    return Row_bamboo_pt::lock_get(type, txn, access);
  };
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt,
      Access * access) override;
};

#endif
