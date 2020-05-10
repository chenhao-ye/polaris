#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

#include "row_bamboo.h"


class Row_bamboo : public Row_bamboo_pt {
 public:
  void init(row_t * row);
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt,
      Access * access) override;

 protected:
  BBLockEntry * remove_descendants(BBLockEntry * en, txn_man * txn) override;

 private:
  RC wound_conflict(lock_t type, txn_man * txn, ts_t ts, bool check_retired, RC status);
  bool wound_txn(BBLockEntry* en, txn_man* txn, bool check_retired);

};

#endif
