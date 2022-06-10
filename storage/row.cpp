#include <mm_malloc.h>
#include "global.h"
#include "table.h"
#include "catalog.h"
#include "row.h"
#include "txn.h"
#include "row_lock.h"
#include "row_ts.h"
#include "row_mvcc.h"
#include "row_hekaton.h"
#include "row_occ.h"
#include "row_tictoc.h"
#include "row_silo.h"
#include "row_silo_prio.h"
#include "row_vll.h"
#include "row_ww.h"
#include "row_bamboo.h"
//#include "row_bamboo_pt.h"
#include "row_ic3.h"
#include "mem_alloc.h"
#include "manager.h"

RC
row_t::init(table_t * host_table, uint64_t part_id, uint64_t row_id) {
  _row_id = row_id;
  _part_id = part_id;
  this->table = host_table;
  Catalog * schema = host_table->get_schema();
  int tuple_size = schema->get_tuple_size();
  data = (char *) _mm_malloc(sizeof(char) * tuple_size, 64);
#if CC_ALG == IC3
  txn_access = NULL;
  orig = NULL;
#endif
  return RCOK;
}


#if CC_ALG == IC3
void
row_t::init_accesses(Access * access) {
  txn_access = access;
}
#endif

void
row_t::init(int size)
{
  data = (char *) _mm_malloc(size, 64);
}

RC
row_t::switch_schema(table_t * host_table) {
  this->table = host_table;
  return RCOK;
}


void row_t::init_manager(row_t * row) {
#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
  manager = (Row_lock *) mem_allocator.alloc(sizeof(Row_lock), _part_id);
#elif CC_ALG == TIMESTAMP
  manager = (Row_ts *) mem_allocator.alloc(sizeof(Row_ts), _part_id);
#elif CC_ALG == MVCC
  manager = (Row_mvcc *) _mm_malloc(sizeof(Row_mvcc), 64);
#elif CC_ALG == HEKATON
  manager = (Row_hekaton *) _mm_malloc(sizeof(Row_hekaton), 64);
#elif CC_ALG == OCC
  manager = (Row_occ *) mem_allocator.alloc(sizeof(Row_occ), _part_id);
#elif CC_ALG == TICTOC
  manager = (Row_tictoc *) _mm_malloc(sizeof(Row_tictoc), 64);
#elif CC_ALG == SILO
  manager = (Row_silo *) _mm_malloc(sizeof(Row_silo), 64);
#elif CC_ALG == SILO_PRIO
  manager = (Row_silo_prio *) _mm_malloc(sizeof(Row_silo_prio), 64);
#elif CC_ALG == ARIA
  manager = (Row_aria *) _mm_malloc(sizeof(Row_aria), 64);
#elif CC_ALG == VLL
  manager = (Row_vll *) mem_allocator.alloc(sizeof(Row_vll), _part_id);
#elif CC_ALG == WOUND_WAIT
  manager = (Row_ww *) mem_allocator.alloc(sizeof(Row_ww), _part_id);
#elif CC_ALG == BAMBOO
  manager = (Row_bamboo *) mem_allocator.alloc(sizeof(Row_bamboo), _part_id);
  new(manager) Row_bamboo();
#elif CC_ALG == IC3
  manager = (Row_ic3 *) _mm_malloc(sizeof(Row_ic3), 64);
#endif

#if CC_ALG != HSTORE
  manager->init(this);
#endif
}

table_t * row_t::get_table() {
  return table;
}

Catalog * row_t::get_schema() {
  return get_table()->get_schema();
}

const char * row_t::get_table_name() {
  return get_table()->get_table_name();
};
uint64_t row_t::get_tuple_size() {
  return get_schema()->get_tuple_size();
}

uint64_t row_t::get_field_cnt() {
  return get_schema()->field_cnt;
}

void row_t::inc_value(int id, uint64_t val) {
  int pos = get_schema()->get_field_index(id);
  ATOM_ADD(data[pos], val);
}

void row_t::dec_value(int id, uint64_t val) {
  int pos = get_schema()->get_field_index(id);
  ATOM_SUB(data[pos], val);
}

void row_t::set_value(int id, void * ptr) {
  int datasize = get_schema()->get_field_size(id);
  int pos = get_schema()->get_field_index(id);
#if CC_ALG == IC3
  // assume no blind writes.
  if (txn_access)
    txn_access->wr_accesses = (txn_access->wr_accesses | (1UL << id));
#endif
  memcpy( &data[pos], ptr, datasize);
  //debugging
  assert(data);
  assert(ptr);
}

void row_t::set_value(int id, void * ptr, int size) {
  int pos = get_schema()->get_field_index(id);
#if CC_ALG == IC3
  // assume no blind writes.
  if (txn_access)
    txn_access->wr_accesses = (txn_access->wr_accesses | (1UL << id));
#endif
  memcpy( &data[pos], ptr, size);
  //debugging
  assert(data);
  assert(ptr);
}

void row_t::set_value(const char * col_name, void * ptr) {
  uint64_t id = get_schema()->get_field_id(col_name);
#if CC_ALG == IC3
  // assume no blind writes.
  if (txn_access)
    txn_access->wr_accesses = (txn_access->wr_accesses | (1UL << id));
#endif
  set_value(id, ptr);
}

SET_VALUE(uint64_t);
SET_VALUE(int64_t);
SET_VALUE(double);
SET_VALUE(UInt32);
SET_VALUE(SInt32);

GET_VALUE(uint64_t);
GET_VALUE(int64_t);
GET_VALUE(double);
GET_VALUE(UInt32);
GET_VALUE(SInt32);

char * row_t::get_value(int idx) {
  uint64_t id = (uint64_t) idx;
#if CC_ALG == IC3 && IC3_FIELD_LOCKING
  if (txn_access) {
    // try to acquire read access
    this->manager->access(this, id, txn_access);
    txn_access->rd_accesses = (txn_access->rd_accesses | (1UL << id));
  }
#endif
  return get_value_plain(id);
}

char * row_t::get_value_plain(uint64_t id) {
  int pos = get_schema()->get_field_index(id);
  return &data[pos];
}

char * row_t::get_value(char * col_name) {
#if CC_ALG == IC3 && IC3_FIELD_LOCKING
  if (txn_access) {
    uint64_t id = get_schema()->get_field_id(col_name);
    this->manager->access(this, id, txn_access);
    txn_access->rd_accesses = (txn_access->rd_accesses | (1UL << id));
    // copy data from orig row
  }
#endif
  uint64_t pos = get_schema()->get_field_index(col_name);
  return &data[pos];
}

char * row_t::get_data() { return data; }

void row_t::set_data(char * data, uint64_t size) {
  memcpy(this->data, data, size);
  assert(data);
  assert(this->data);
}
// copy from the src to this
void row_t::copy(row_t * src) {
  set_data(src->get_data(), src->get_tuple_size());
}

void row_t::set_value_plain(int idx, void * ptr) {
  int datasize = get_schema()->get_field_size(idx);
  int pos = get_schema()->get_field_index(idx);
  memcpy(&data[pos], ptr, datasize);
  //debugging
  assert(data);
  assert(ptr);
}

void row_t::copy(row_t * src, int idx) {
  char * ptr = src->get_value_plain(idx);
  set_value_plain(idx, ptr);
  //debugging
  assert(data);
  assert(src->data);
}

void row_t::free_row() {
  free(data);
}

#if CC_ALG == BAMBOO
RC row_t::retire_row(BBLockEntry * lock_entry) {
  return this->manager->lock_retire(lock_entry);
}
#endif

RC row_t::get_row(access_t type, txn_man * txn, row_t *& row, Access * access) {
  RC rc = RCOK;
#if CC_ALG == IC3
  this->manager->access(access->data, access);
  return rc;
#elif CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT || CC_ALG == WOUND_WAIT || CC_ALG == BAMBOO
  uint64_t thd_id = txn->get_thd_id();
  lock_t lt = (type == RD || type == SCAN)? LOCK_SH : LOCK_EX;
  #if CC_ALG == DL_DETECT
  uint64_t * txnids;
  int txncnt;
  rc = this->manager->lock_get(lt, txn, txnids, txncnt);
  #elif CC_ALG == BAMBOO || CC_ALG == WOUND_WAIT
  if (txn->lock_abort) {
    row = NULL;
    return Abort;
  }
  rc = this->manager->lock_get(lt, txn, access);
  #else
  assert(txn->get_ts() != 0);
  rc = this->manager->lock_get(lt, txn, access);
  #endif
  if (rc == RCOK) {
  } else if (rc == Abort) {
    row = NULL;
    return rc;
  } else if (rc == WAIT) {
    ASSERT(CC_ALG == WAIT_DIE || CC_ALG == DL_DETECT || CC_ALG == WOUND_WAIT || CC_ALG == BAMBOO);
    uint64_t starttime = get_sys_clock();
    #if CC_ALG == DL_DETECT
    bool dep_added = false;
    #endif
    uint64_t endtime;
    #if (CC_ALG != WOUND_WAIT) && (CC_ALG != BAMBOO)
    txn->lock_abort = false;
    #endif
    INC_STATS(txn->get_thd_id(), wait_cnt, 1);
    while (!txn->lock_ready && !txn->lock_abort)
    {
    #if CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT) || (CC_ALG == BAMBOO)
      continue;
    #elif CC_ALG == DL_DETECT
      uint64_t last_detect = starttime;
      uint64_t last_try = starttime;
      uint64_t now = get_sys_clock();
      if (now - starttime > g_timeout ) {
				txn->lock_abort = true;
				break;
      }
      if (g_no_dl) {
				PAUSE
				continue;
      }
      int ok = 0;
      if ((now - last_detect > g_dl_loop_detect) && (now - last_try > DL_LOOP_TRIAL)) {
        if (!dep_added) {
          ok = dl_detector.add_dep(txn->get_txn_id(), txnids, txncnt, txn->row_cnt);
	  if (ok == 0)
            dep_added = true;
          else if (ok == 16)
	  last_try = now;
        }
	if (dep_added) {
	  ok = dl_detector.detect_cycle(txn->get_txn_id());
	  if (ok == 16)  // failed to lock the deadlock detector
	    last_try = now;
	  else if (ok == 0) 
	    last_detect = now;
          else if (ok == 1) {
	    last_detect = now;
          }
        }
      } else 
        PAUSE
    #endif
    }
    if (txn->lock_ready) {
      rc = RCOK;
    } else if (txn->lock_abort) {
      // only possible for wound-wait based algs.
      // check if txn is aborted, if aborted due to conflicts on this or other
      // try to release lock
#if (CC_ALG == WOUND_WAIT) || (CC_ALG == BAMBOO)
      return_row(access->lock_entry, Abort);
#endif
      return Abort;
    }
    endtime = get_sys_clock();
    INC_TMP_STATS(thd_id, time_wait, endtime - starttime);
  } else if (rc == FINISH) {
    // RAW optimization, need to return data for read
  }
  row = this;
  return rc;
#elif CC_ALG == TIMESTAMP || CC_ALG == MVCC || CC_ALG == HEKATON
  uint64_t thd_id = txn->get_thd_id();
	// For TIMESTAMP RD, a new copy of the row will be returned.
	// for MVCC RD, the version will be returned instead of a copy
	// So for MVCC RD-WR, the version should be explicitly copied.
	//row_t * newr = NULL;
  #if CC_ALG == TIMESTAMP
	// TODO. should not call malloc for each row read. Only need to call malloc once 
	// before simulation starts, like TicToc and Silo.
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t), this->get_part_id());
	txn->cur_row->init(get_table(), this->get_part_id());
  #endif

	// TODO need to initialize the table/catalog information.
	TsType ts_type = (type == RD)? R_REQ : P_REQ; 
	rc = this->manager->access(txn, ts_type, row);
	if (rc == RCOK ) {
		row = txn->cur_row;
	} else if (rc == WAIT) {
		uint64_t t1 = get_sys_clock();
		while (!txn->ts_ready)
			PAUSE
		uint64_t t2 = get_sys_clock();
		INC_TMP_STATS(thd_id, time_wait, t2 - t1);
		row = txn->cur_row;
	}
	if (rc != Abort) {
		row->table = get_table();
		assert(row->get_schema() == this->get_schema());
	}
	return rc;
#elif CC_ALG == OCC
	// OCC always make a local copy regardless of read or write
	txn->cur_row = (row_t *) mem_allocator.alloc(sizeof(row_t), get_part_id());
	txn->cur_row->init(get_table(), get_part_id());
	rc = this->manager->access(txn, R_REQ);
	row = txn->cur_row;
	return rc;
#elif CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == SILO_PRIO || CC_ALG == ARIA
	// like OCC, tictoc also makes a local copy for each read/write
	row->table = get_table();
	TsType ts_type = (type == RD)? R_REQ : P_REQ; 
	rc = this->manager->access(txn, ts_type, row);
	return rc;
#elif CC_ALG == HSTORE || CC_ALG == VLL
	row = this;
	return rc;
#else
	assert(false);
	return rc;
#endif
}

#if CC_ALG == BAMBOO
void row_t::return_row(BBLockEntry * lock_entry, RC rc) {
    this->manager->lock_release(lock_entry, rc);
}
#elif CC_ALG == WOUND_WAIT
void row_t::return_row(LockEntry * lock_entry, RC rc) {
    this->manager->lock_release(lock_entry);
}
#endif

#if CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
void row_t::return_row(access_t type, row_t * row, LockEntry * lock_entry) {
#if CC_ALG == WOUND_WAIT
  // make committed writes globally visible
  if (type == WR) // must be commited, aborted write will be XP
    this->copy(row);
  this->manager->lock_release(lock_entry);
#elif CC_ALG == WAIT_DIE || (CC_ALG == NO_WAIT) || (CC_ALG == DL_DETECT)
  assert (row == NULL || row == this || type == XP);
  if (type == XP) {// recover from previous writes.
    this->copy(row);
  }
  this->manager->lock_release(lock_entry);
#else
  assert(false);
#endif
}
#endif

// the "row" is the row read out in get_row(). 
// For locking based CC_ALG, the "row" is the same as "this". 
// For timestamp based CC_ALG, the "row" != "this", and the "row" must be freed.
// For MVCC, the row will simply serve as a version. The version will be 
// delete during history cleanup.
// For TIMESTAMP, the row will be explicity deleted at the end of access().
// (cf. row_ts.cpp)
void row_t::return_row(access_t type, txn_man * txn, row_t * row) {
#if CC_ALG == TIMESTAMP || CC_ALG == MVCC
  // for RD or SCAN or XP, the row should be deleted.
	// because all WR should be companied by a RD
	// for MVCC RD, the row is not copied, so no need to free. 
  #if CC_ALG == TIMESTAMP
	if (type == RD || type == SCAN) {
		row->free_row();
		mem_allocator.free(row, sizeof(row_t));
	}
  #endif
	if (type == XP) {
		this->manager->access(txn, XP_REQ, row);
	} else if (type == WR) {
		assert (type == WR && row != NULL);
		assert (row->get_schema() == this->get_schema());
		RC rc = this->manager->access(txn, W_REQ, row);
		assert(rc == RCOK);
	}
#elif CC_ALG == OCC
  assert (row != NULL);
	if (type == WR)
		manager->write( row, txn->end_ts );
	row->free_row();
	mem_allocator.free(row, sizeof(row_t));
	return;
#elif CC_ALG == TICTOC || CC_ALG == SILO || CC_ALG == SILO_PRIO || CC_ALG == ARIA
  assert (row != NULL);
	return;
#elif CC_ALG == HSTORE || CC_ALG == VLL
  return;
#else
  assert(false);
#endif
}


