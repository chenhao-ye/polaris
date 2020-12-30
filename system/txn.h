#pragma once

#include "global.h"
#include "helper.h"

class workload;
class thread_t;
class row_t;
class table_t;
class base_query;
class INDEX;
class txn_man;

// each thread has a txn_man. 
// a txn_man corresponds to a single transaction.

//For VLL
enum TxnType {VLL_Blocked, VLL_Free};

class Access {
 public:
  access_t 	type;
  row_t * 	orig_row;
  row_t * 	data;
  row_t * 	orig_data;
#if COMMUTATIVE_OPS
  // support increment-only for now
  uint64_t  com_val;
  int       com_col;
  com_t     com_op;
#endif
  void cleanup();
#if CC_ALG == TICTOC
  ts_t 		wts;
  ts_t 		rts;
#elif CC_ALG == SILO
  ts_t 		tid;
  ts_t 		epoch;
#elif CC_ALG == HEKATON
  void * 	history_entry;
#elif CC_ALG == IC3
  ts_t *    tids;
  ts_t      epochs;
  uint64_t  tid;
  uint64_t  rd_accesses;
  uint64_t  wr_accesses;
  uint64_t  lk_accesses;
#endif
  void * lock_entry;
};

#if CC_ALG == IC3
struct TxnEntry {
  txn_man * txn;
  uint64_t txn_id;
};
#endif

class txn_man
{
 public:
  virtual void init(thread_t * h_thd, workload * h_wl, uint64_t part_id);
  void release();
  thread_t * h_thd;
  workload * h_wl;
  myrand * mrand;
  uint64_t abort_cnt;
#if DEBUG_ABORT_LENGTH
  uint64_t      abort_chain;
#endif

  virtual RC 		run_txn(base_query * m_query) = 0;
  uint64_t 		    get_thd_id();
  workload * 		get_wl();
  void 			    set_txn_id(txnid_t txn_id);
  txnid_t 		    get_txn_id();

  // [COMMUTATIVE OPERATIONS]
#if COMMUTATIVE_OPS
  void             inc_value(int col, uint64_t val);
  void             dec_value(int col, uint64_t val);
#endif

  // [WW, BAMBOO]
  status_t              wound_txn(txn_man * txn);
  status_t              set_abort()
  {
#if CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT || CC_ALG == IC3
    if (ATOM_CAS(status, RUNNING, ABORTED)) {
        lock_abort = true;
	return ABORTED;
    } else {
#if BB_PRECOMMIT
        if (ATOM_CAS(status, PRECOMMIT, ABORTED)) {
            lock_abort = true;
        }
#else
	return status;
#endif
    }
#else
    return ABORTED;
#endif
  };

  void          increment_commit_barriers();
  void			decrement_commit_barriers();
  bool			atomic_set_ts(ts_t ts);
  ts_t			set_next_ts(int n);
  void			reassign_ts();
  void 			set_ts(ts_t timestamp);

  ts_t 			get_ts();

  pthread_mutex_t txn_lock;
  row_t * volatile cur_row;
#if CC_ALG == HEKATON
  void * volatile history_entry;
#endif
  // [DL_DETECT, NO_WAIT, WAIT_DIE, WOUND_WAIT, BAMBOO]
  volatile bool 	lock_ready;
  volatile bool 	lock_abort; // forces another waiting txn to abort.
  // [BAMBOO]
  status_t          status; // RUNNING, COMMITED, ABORTED
  // [TIMESTAMP, MVCC]
  bool volatile     ts_ready;
  // [HSTORE]
  int volatile 	    ready_part;
  RC 			    finish(RC rc);
  void 			    cleanup(RC rc);
#if CC_ALG == TICTOC
  ts_t 			get_max_wts() 	{ return _max_wts; }
  void 			update_max_wts(ts_t max_wts);
  ts_t 			last_wts;
  ts_t 			last_rts;
#elif CC_ALG == SILO
  ts_t 			last_tid;
#elif CC_ALG == IC3
  TPCCTxnType           curr_type;
  volatile int  curr_piece;
  void          begin_piece(int piece_id);
  RC            end_piece(int piece_id);
  void          abort_ic3();
  int           get_txn_pieces(int tpe);
#endif

  // For OCC
  uint64_t 		    start_ts;
  uint64_t 		    end_ts;
  // following are public for OCC
  int 			    row_cnt;
  int	 		    wr_cnt;
  Access **		    accesses;
  int 			    num_accesses_alloc;

  // For VLL
  TxnType 		    vll_txn_type;
  itemid_t *	    index_read(INDEX * index, idx_key_t key, int part_id);
  void 			    index_read(INDEX * index, idx_key_t key, int part_id,
                                 itemid_t *& item);
  row_t * 		    get_row(row_t * row, access_t type);

  // For BAMBOO
#if CC_ALG == BAMBOO
  RC                retire_row(int access_cnt);
  ts_t              get_exec_time() {return get_sys_clock() - start_ts;};
#endif

 protected:
  void 			    insert_row(row_t * row, table_t * table);
  void              index_insert(row_t * row, INDEX * index, idx_key_t key);
 private:
#if CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
  void              assign_lock_entry(Access * access);
#endif
  // insert rows
  uint64_t 		    insert_cnt;
  row_t * 		    insert_rows[MAX_ROW_PER_TXN];
  txnid_t 		    txn_id;
  ts_t volatile		timestamp;
  int volatile      commit_barriers;

  bool              _write_copy_ptr;
#if CC_ALG == TICTOC || CC_ALG == SILO
  bool 			    _pre_abort;
  bool 			    _validation_no_wait;
#endif
#if CC_ALG == TICTOC
  bool			    _atomic_timestamp;
  ts_t 			    _max_wts;
  // the following methods are defined in concurrency_control/tictoc.cpp
  RC				validate_tictoc();
#elif CC_ALG == SILO
  ts_t 			    _cur_tid;
  RC				validate_silo();
#elif CC_ALG == HEKATON
  RC 				validate_hekaton(RC rc);
#elif CC_ALG == IC3
  int               access_marker;
  TxnEntry **       depqueue;
  int               depqueue_sz;
  RC                validate_ic3();
  uint64_t          piece_starttime;
#endif
};

#include "thread.h"

inline status_t txn_man::wound_txn(txn_man * txn)
{
#if CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT
    if (status != RUNNING)
	    return COMMITED;
#if BB_PRECOMMIT
    // CANNOT wound PRECOMMITTED txn
    if (ATOM_CAS(txn->status, RUNNING, ABORTED)) {
        lock_abort = true;
        return ABORTED;
    }
    if (txn->status != ABORTED)
	    return COMMITED;
    return ABORTED;
#else
    return txn->set_abort();
#endif
#else
  return false;
#endif
}
