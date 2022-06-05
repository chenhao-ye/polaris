#pragma once

#include "global.h"

#define DECL_SET_VALUE(type) \
	void set_value(int col_id, type value);

#define SET_VALUE(type) \
	void row_t::set_value(int col_id, type value) { \
		set_value(col_id, &value); \
	}

#define DECL_GET_VALUE(type)\
	void get_value(int col_id, type & value);

#define GET_VALUE(type)\
	void row_t::get_value(int col_id, type & value) { \
          value = *(type *)get_value(col_id); \
        }

//		int pos = get_schema()->get_field_index(col_id);
//		value = *(type *)&data[pos];
//	}

class Access;
class table_t;
class Catalog;
class txn_man;
class Row_lock;
class Row_mvcc;
class Row_hekaton;
class Row_ts;
class Row_occ;
class Row_tictoc;
class Row_silo;
class Row_silo_prio;
class Row_aria;
class Row_vll;
class Row_ww;
class Row_bamboo;
//class Row_bamboo_pt;
class Row_ic3;
#if CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
struct LockEntry;
#elif CC_ALG == BAMBOO
struct BBLockEntry;
#endif

class row_t
{
  public:

    RC init(table_t * host_table, uint64_t part_id, uint64_t row_id = 0);
    void init(int size);
    RC switch_schema(table_t * host_table);
    // not every row has a manager
    void init_manager(row_t * row);

    table_t * get_table();
    Catalog * get_schema();
    const char * get_table_name();
    uint64_t get_field_cnt();
    uint64_t get_tuple_size();
    uint64_t get_row_id() { return _row_id; };

    void copy(row_t * src);
    void copy(row_t * src, int idx);

    void 		set_primary_key(uint64_t key) { _primary_key = key; };
    uint64_t 	get_primary_key() {return _primary_key; };
    uint64_t 	get_part_id() { return _part_id; };

    void set_value(int id, void * ptr);
    void set_value_plain(int id, void * ptr);
    void set_value(int id, void * ptr, int size);
    void set_value(const char * col_name, void * ptr);
    char * get_value(int id);
    char * get_value_plain(uint64_t id);
    char * get_value(char * col_name);
    void inc_value(int id, uint64_t val);
    void dec_value(int id, uint64_t val);

    DECL_SET_VALUE(uint64_t);
    DECL_SET_VALUE(int64_t);
    DECL_SET_VALUE(double);
    DECL_SET_VALUE(UInt32);
    DECL_SET_VALUE(SInt32);

    DECL_GET_VALUE(uint64_t);
    DECL_GET_VALUE(int64_t);
    DECL_GET_VALUE(double);
    DECL_GET_VALUE(UInt32);
    DECL_GET_VALUE(SInt32);


    void set_data(char * data, uint64_t size);
    char * get_data();

    void free_row();

    // for concurrency control. can be lock, timestamp etc.
#if CC_ALG == BAMBOO
    RC retire_row(BBLockEntry * lock_entry);
#elif CC_ALG == IC3
    row_t * orig;
    void init_accesses(Access * access);
    Access * txn_access; // only used when row is a local copy
#endif
    RC get_row(access_t type, txn_man * txn, row_t *& row, Access *access=NULL);
#if CC_ALG == BAMBOO
    void return_row(BBLockEntry * lock_entry, RC rc);
#elif CC_ALG == WOUND_WAIT
    void return_row(LockEntry * lock_entry, RC rc);
#endif
#if CC_ALG == WOUND_WAIT || CC_ALG == WAIT_DIE || CC_ALG == NO_WAIT || CC_ALG == DL_DETECT
    void return_row(access_t type, row_t * row, LockEntry * lock_entry);
#endif
    void return_row(access_t type, txn_man * txn, row_t * row);

#if CC_ALG == DL_DETECT || CC_ALG == NO_WAIT || CC_ALG == WAIT_DIE
    Row_lock * manager;
#elif CC_ALG == TIMESTAMP
    Row_ts * manager;
  #elif CC_ALG == MVCC
  	Row_mvcc * manager;
  #elif CC_ALG == HEKATON
  	Row_hekaton * manager;
  #elif CC_ALG == OCC
  	Row_occ * manager;
  #elif CC_ALG == TICTOC
  	Row_tictoc * manager;
  #elif CC_ALG == SILO
  	Row_silo * manager;
  #elif CC_ALG == SILO_PRIO
  	Row_silo_prio * manager;
  #elif CC_ALG == ARIA
  	Row_aria * manager;
  #elif CC_ALG == VLL
  	Row_vll * manager;
  #elif CC_ALG == WOUND_WAIT
  	Row_ww * manager;
  #elif CC_ALG == BAMBOO
	Row_bamboo * manager;
  #elif CC_ALG == IC3
	Row_ic3 * manager;
#endif
    char * data;
    table_t * table;
  private:
    // primary key should be calculated from the data stored in the row.
    uint64_t 		_primary_key;
    uint64_t		_part_id;
    uint64_t 		_row_id;
};
