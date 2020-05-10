#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

#include "row_bamboo_pt.h"


class Row_bamboo : public Row_bamboo_pt {
 public:
  void init(row_t * row);
  RC lock_get(lock_t type, txn_man * txn, Access * access) override {
    return Row_bamboo_pt::lock_get(type, txn, access);
  };
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt,
      Access * access) override;

 private:
  RC wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status);
  bool wound_txn(BBLockEntry* en, txn_man* txn, bool check_retired);
  ts_t local_ts;
  ts_t txn_ts;
};

#endif
