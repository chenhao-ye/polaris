#pragma once

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;
class Row_ic3;

#define LOCK_BIT (1UL << 63)
#if CC_ALG == IC3

struct IC3LockEntry {
  access_t             type;
  txn_man *            txn;
  uint64_t             txn_id;
  IC3LockEntry *          prev;
  IC3LockEntry *          next;
};

class Cell_ic3 {
 public:
  void                  init(row_t * orig_row, uint64_t id);
  /* copy to corresponding col of local row */
  void                  access(row_t * local_row, Access *txn_access);
  uint64_t              get_tid() {return _tid;};
  void                  add_to_acclist(txn_man * txn, access_t type);
  void                  rm_from_acclist(txn_man * txn, bool aborted);
  IC3LockEntry *        get_last_writer();
  IC3LockEntry *        get_last_accessor();
  bool                  try_lock();
  void                  release();
  void                  update_version(uint64_t txn_id) {_tid = txn_id;};
 private:
  row_t * 			    _row;
  Row_ic3 *             row_manager;
  volatile uint64_t	    _tid;
  uint64_t              idx;
  int                   acclist_cnt;
  IC3LockEntry *        acclist;
  IC3LockEntry *        acclist_tail;
  volatile int          lock;
  /*
#if LATCH == LH_SPINLOCK
  pthread_spinlock_t *  latch;
#elif LATCH == LH_MUTEX
  pthread_mutex_t *     latch;
#else
  mcslock *             latch;
#endif
*/

};

class Row_ic3 {
 public:
  void 	            init(row_t * row);
#if IC3_FIELD_LOCKING
  bool              try_lock(uint64_t idx) {return cell_managers[idx].try_lock();};
  void              release(uint64_t idx) {cell_managers[idx].release();};
  uint64_t          get_tid(uint64_t idx) {return cell_managers[idx].get_tid();};
  IC3LockEntry *    get_last_writer(uint64_t idx) {
    return cell_managers[idx].get_last_writer();};
  IC3LockEntry *    get_last_accessor(uint64_t idx) {
    return cell_managers[idx].get_last_accessor();};
  void              add_to_acclist(uint64_t idx, txn_man * txn, access_t type) {
    cell_managers[idx].add_to_acclist(txn, type);};
  void              rm_from_acclist(uint64_t idx, txn_man * txn, bool aborted=false) {
    cell_managers[idx].rm_from_acclist(txn, aborted);};
  void              update_version(uint64_t idx, uint64_t txn_id) {
    cell_managers[idx].update_version(txn_id);};
  void              access(row_t * local_row, uint64_t idx, Access * txn_access) {
    cell_managers[idx].access(local_row, txn_access);};
#else // tuple-level locking
  bool              try_lock();
  uint64_t          get_tid() {return _tid;};
  IC3LockEntry *    get_last_writer();
  IC3LockEntry *    get_last_accessor();
  void              release() {lock = 0;};
  void              add_to_acclist(txn_man * txn, access_t type);
  void              rm_from_acclist(txn_man * txn, bool aborted=false);
  void              update_version(uint64_t txn_id) {_tid = txn_id;};
  void              access(row_t * local_row, Access * txn_access);
#endif
  row_t * 			    _row;

 private:
#if !IC3_FIELD_LOCKING
  volatile uint64_t	_tid;
  uint64_t              idx;
  int                   acclist_cnt;
  IC3LockEntry *        acclist;
  IC3LockEntry *        acclist_tail;
  volatile int          lock;
#else
  Cell_ic3 *            cell_managers;
#endif
};

#endif
