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
#if LATCH == LH_SPINLOCK
  latch = new pthread_spinlock_t;
  pthread_spin_init(latch, PTHREAD_PROCESS_SHARED);
#elif LATCH == LH_MUTEX
  latch = new pthread_mutex_t;
  pthread_mutex_init(latch, NULL);
#else
  latch = new mcslock();
#endif
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
  //printf("access row %lu cell %d v = %lu, _tid = %lu\n", _row->get_row_id(), idx, v, _tid);
}

bool
Cell_ic3::try_lock() {
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
  return true;
}

void
Cell_ic3::release() {
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
}

void
Cell_ic3::add_to_acclist(txn_man * txn, access_t type) {
  // get new lock entry
  IC3LockEntry * new_entry = (IC3LockEntry *) mem_allocator.alloc(sizeof(IC3LockEntry),
      _row->get_part_id());
  new_entry->type = type;
  new_entry->txn_id = txn->get_txn_id();
  new_entry->txn = txn;
  new_entry->prev = NULL;
  new_entry->next = NULL;
  // add to tail
  LIST_PUT_TAIL(acclist, acclist_tail, new_entry);
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
Cell_ic3::rm_from_acclist(txn_man * txn) {
  IC3LockEntry * en = acclist;
  while(en != NULL) {
    if (en->txn == txn)
      break;
    en = en->next;
  }
  if (en) {
    LIST_REMOVE_HT(en, acclist, acclist_tail);
  }
}

//////////////////////// ROW_IC3 ////////////////////////
void
Row_ic3::init(row_t * row)
{
  _row = row;
  //_tid_word = 0; // not used by cell-level lock
  cell_managers = (Cell_ic3 *) _mm_malloc(sizeof(Cell_ic3)
      *row->get_field_cnt(), 64);
  for (UInt32 i = 0; i < row->get_field_cnt(); i++) {
    cell_managers[i].init(_row,(int)i);
  }
}

/*
RC
Row_ic3::access(txn_man * txn, row_t * local_row) {
  // called only if using tuple-level locks
  uint64_t v = 0;
  uint64_t v2 = 1;
  while (v2 != v) {
    v = _tid_word;
    while (v & LOCK_BIT) {
      PAUSE
          v = _tid_word;
    }
    local_row->copy(_row);
    COMPILER_BARRIER
    v2 = _tid_word;
  }
  //txn->last_tid = _tid_word;
  return RCOK;
}*/

void
Row_ic3::access(row_t * local_row, uint64_t idx, Access * txn_access) {
  cell_managers[idx].access(local_row, txn_access);
}

bool
Row_ic3::try_lock(uint64_t idx) {
  return cell_managers[idx].try_lock();
}


///////////////////  fen ge xian ///////////////////

/*
bool
Row_ic3::validate(ts_t tid, bool in_write_set) {
  uint64_t v = _tid_word;
  if (in_write_set)
    return tid == (v & (~LOCK_BIT));

  if (v & LOCK_BIT)
    return false;
  else if (tid != (v & (~LOCK_BIT)))
    return false;
  else
    return true;
}

void
Row_ic3::write(row_t * data, uint64_t tid) {
  _row->copy(data);
  uint64_t v = _tid_word;
  M_ASSERT(tid > (v & (~LOCK_BIT)) && (v & LOCK_BIT), "tid=%ld, v & LOCK_BIT=%ld, v & (~LOCK_BIT)=%ld\n", tid, (v & LOCK_BIT), (v & (~LOCK_BIT)));
  _tid_word = (tid | LOCK_BIT);
}

void
Row_silo::lock() {
  uint64_t v = _tid_word;
  while ((v & LOCK_BIT) || !__sync_bool_compare_and_swap(&_tid_word, v, v | LOCK_BIT)) {
    PAUSE
        v = _tid_word;
  }
}

void
Row_silo::release() {
#if ATOMIC_WORD
  assert(_tid_word & LOCK_BIT);
	_tid_word = _tid_word & (~LOCK_BIT);
#else
  pthread_mutex_unlock( _latch );
#endif
}

bool
Row_silo::try_lock()
{
#if ATOMIC_WORD
  uint64_t v = _tid_word;
	if (v & LOCK_BIT) // already locked
		return false;
	return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
#else
  return pthread_mutex_trylock( _latch ) != EBUSY;
#endif
}

uint64_t
Row_silo::get_tid()
{
  assert(ATOMIC_WORD);
  return _tid_word & (~LOCK_BIT);
}
*/

#endif
