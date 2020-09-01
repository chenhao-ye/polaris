#pragma once 

class table_t;
class Catalog;
class txn_man;
struct TsReqEntry;
class Row_ic3;

#if CC_ALG == IC3
#define LOCK_BIT (1UL << 63)

class Cell_ic3 {
 public:
  void                  init(row_t * orig_row, int id);
  /* copy to corresponding col of local row */
  RC                    access(row_t * local_row, Access *txn_access);
 private:
  row_t * 			    _row;
  Row_ic3 *             row_manager;
  volatile uint64_t	    _tid_word;
  int                   idx;
};

class Row_ic3 {
public:
	void 				init(row_t * row);
	RC 					access(txn_man * txn, row_t * local_row); // row-level
	RC                  access(row_t * local_row, int idx, Access * txn_access);	// cell-level
	void 				assert_lock() {assert(_tid_word & LOCK_BIT); }
private:
    Cell_ic3 *          cell_managers;
	volatile uint64_t	_tid_word;
	row_t * 			_row;
};

#endif
