#include "txn.h"
#include "row.h"
#include "row_ic3.h"
#include "mem_alloc.h"

#if CC_ALG==IC3
void
Cell_ic3::init(row_t * orig_row, uint64_t id) {
  _row = orig_row;
  row_manager = orig_row->manager;
  _tid = 0;
  idx = id;
  acclist = NULL;
  acclist_tail = NULL;
  acclist_cnt = 0;
  lock = 0;
  /*
#if LATCH == LH_SPINLOCK
  latch = new pthread_spinlock_t;
  pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#elif LATCH == LH_MUTEX
  latch = new pthread_mutex_t;
  pthread_mutex_init(latch, NULL);
#else
  latch = new mcslock();
#endif
*/
}

void
Cell_ic3::access(row_t * local_row, Access * txn_access) {
  // called only if using cell-level locks
  uint64_t v = 0;
  uint64_t v2 = 1;
  while (v2 != v) {
    v = _tid;
    // copy cell value from orig row
    local_row->copy(_row, idx);
    COMPILER_BARRIER
    v2 = _tid;
  }
  txn_access->tids[idx] = v;
  //printf("access %p-%lu, v = %lu, _tid = %lu\n", _row, idx, v, _tid);
}

bool
Cell_ic3::try_lock() {
/*
#if THREAD_CNT > 1
#if LATCH == LH_SPINLOCK
  pthread_spin_lock( latch );
#elif LATCH == LH_MUTEX
  pthread_mutex_lock( latch );
#else
  printf("MCS Lock is not supported for IC3 yet.\n");
  assert(false);
  //latch->acquire(en->m_node);
#endif
#endif
*/
  if (lock == 1)
    return false;
  return ATOM_CAS(lock, 0, 1);
}

void
Cell_ic3::release() {
	/*
#if THREAD_CNT > 1
#if LATCH == LH_SPINLOCK
  pthread_spin_unlock( latch );
#elif LATCH == LH_MUTEX
  pthread_mutex_unlock( latch );
#else
  printf("MCS Lock is not supported for IC3 yet.\n");
  assert(false);
  //latch->release(en->m_node);
#endif
#endif
*/
  lock = 0;
}

void
Cell_ic3::add_to_acclist(txn_man * txn, access_t type) {
  // get new lock entry
  IC3LockEntry * new_entry = (IC3LockEntry *) mem_allocator.alloc(sizeof(IC3LockEntry), _row->get_part_id());
  new_entry->type = type;
  new_entry->txn_id = txn->get_txn_id();
  new_entry->txn = txn;
  new_entry->prev = NULL;
  new_entry->next = NULL;
  // add to tail
  LIST_PUT_TAIL(acclist, acclist_tail, new_entry);
  acclist_cnt++;
}

IC3LockEntry *
Cell_ic3::get_last_writer() {
  IC3LockEntry * en = acclist_tail;
  while(en != NULL) {
    if (en->type == WR)
      break;
    en = en->prev;
  }
  return en;
}

IC3LockEntry *
Cell_ic3::get_last_accessor() {
  if (acclist_tail)
    return acclist_tail;
  return NULL;
}

void
Cell_ic3::rm_from_acclist(txn_man * txn, bool aborted) {
  // modifying acclist, acquire latch
  while(try_lock() == false)
	  continue;
  IC3LockEntry * en = acclist;
  IC3LockEntry * to_rm = NULL;
  bool set_abort = false;
  while(en != NULL) {
    if (en->txn == txn) {
      to_rm = en;
      if (aborted && (en->type == WR))
        set_abort = true;
      else
        break;
    } else if (set_abort) {
      assert(en->txn->set_abort() == ABORTED);
    }
    en = en->next;
  }
  if (to_rm) {
    LIST_REMOVE_HT(to_rm, acclist, acclist_tail);
    acclist_cnt--;
    free(to_rm);
  }
  release();
}

//////////////////////// ROW_IC3 ////////////////////////
void
Row_ic3::init(row_t * row)
{
  _row = row;
#if IC3_FIELD_LOCKING
  cell_managers = (Cell_ic3 *) _mm_malloc(sizeof(Cell_ic3)
      *row->get_field_cnt(), 64);
  for (UInt32 i = 0; i < row->get_field_cnt(); i++) {
    cell_managers[i].init(row,(int)i);
  }
#else // tuple-level locking
  _tid = 0;
  acclist = NULL;
  acclist_tail = NULL;
  acclist_cnt = 0;
  lock = 0;
#endif
}

#if !IC3_FIELD_LOCKING
bool
Row_ic3::try_lock() {
  if (lock == 1)
    return false;
  return ATOM_CAS(lock, 0, 1);
}

IC3LockEntry *
Row_ic3::get_last_writer() {
  IC3LockEntry * en = acclist_tail;
  while(en != NULL) {
    if (en->type == WR)
      break;
    en = en->prev;
  }
  return en;
}

IC3LockEntry *
Row_ic3::get_last_accessor() {
  if (acclist_tail)
    return acclist_tail;
  return NULL;
}

void
Row_ic3::add_to_acclist(txn_man * txn, access_t type) {
  // get new lock entry
  IC3LockEntry * new_entry = (IC3LockEntry *) mem_allocator.alloc(sizeof(IC3LockEntry), _row->get_part_id());
  new_entry->type = type;
  new_entry->txn_id = txn->get_txn_id();
  new_entry->txn = txn;
  new_entry->prev = NULL;
  new_entry->next = NULL;
  // add to tail
  LIST_PUT_TAIL(acclist, acclist_tail, new_entry);
  acclist_cnt++;
}

void
Row_ic3::rm_from_acclist(txn_man * txn, bool aborted) {
  // modifying acclist, acquire latch
  while(try_lock() == false)
    continue;
  IC3LockEntry * en = acclist;
  IC3LockEntry * to_rm = NULL;
  bool set_abort = false;
  while(en != NULL) {
    if (en->txn == txn) {
      to_rm = en;
      if (aborted && (en->type == WR))
        set_abort = true;
      else
        break;
    } else if (set_abort) {
      assert(en->txn->set_abort() == ABORTED);
    }
    en = en->next;
  }
  if (to_rm) {
    LIST_REMOVE_HT(to_rm, acclist, acclist_tail);
    acclist_cnt--;
    free(to_rm);
  }
  release();
}

void
Row_ic3::access(row_t * local_row, Access * txn_access) {
  uint64_t v = 0;
  uint64_t v2 = 1;
  while (v2 != v) {
    v = _tid;
    // copy from orig row to local row
    local_row->copy(_row);
    COMPILER_BARRIER
    v2 = _tid;
  }
  txn_access->tid = v;
}

#endif // Tuple-level locking
#endif // IC3
