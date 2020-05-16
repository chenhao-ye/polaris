#ifndef ROW_BAMBOO_H
#define ROW_BAMBOO_H

#include "row_bamboo_pt.h"

#define TRY_WOUND(en, check_retired) { \
  if (type == LOCK_SH && !fcw && BB_OPT_RAW) { \
    fcw = en; \
    return FINISH; \
  } \
  if (txn->wound_txn(en->txn) == COMMITED) \
    return Abort; \
  if (check_retired) { \
    en = rm_from_retired(en, true); \
  } else { \
    LIST_RM(owners, owners_tail, en, owner_cnt); \
    return_entry(en); } \
}

#define TRY_WOUND_CONFLICT(en, en_ts, txn_ts, check_retired) { \
  if (status == RCOK && conflict_lock(en->type, type) && (en_ts > txn_ts)) { \
    status = WAIT; \
  } \
  if (status == WAIT && (en_ts > txn_ts)) { \
    TRY_WOUND(en, check_retired); \
  } else { \
    en = en->next; \
  } \
}


class Row_bamboo : public Row_bamboo_pt {
 public:
  void init(row_t * row) override;
  RC lock_get(lock_t type, txn_man * txn, Access * access) override {
    return Row_bamboo_pt::lock_get(type, txn, access);
  };
  RC lock_get(lock_t type, txn_man * txn, uint64_t* &txnids, int &txncnt,
      Access * access) override;

 private:
  RC wound_conflict(lock_t type, txn_man * txn, bool unassigned,
      bool check_retired, RC status);
  // bool wound_txn(BBLockEntry* en, txn_man* txn, bool check_retired);
  ts_t local_ts;
  ts_t txn_ts;
};

#endif
