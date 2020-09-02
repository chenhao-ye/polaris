#include "txn.h"
#include "row.h"
#include "row_silo.h"
#include "mem_alloc.h"

#if CC_ALG==IC3
void
Cell_ic3::init(row_t * orig_row, int id) {
  _row = orig_row;
  row_manager = orig_row->row_manager;
  _tid_word = 0;
  idx = id;
}

RC
Cell_ic3::access(row_t * local_row, Access * txn_access) {
  // called only if using cell-level locks
  uint64_t v = 0;
  uint64_t v2 = 1;
  while (v2 != v) {
    v = _tid_word;
    while (v & LOCK_BIT) {
      PAUSE
      v = _tid_word;
    }
    // copy cell value from orig row
    local_row->copy(_row, idx);
    COMPILER_BARRIER
    v2 = _tid_word;
  }
  txn_access->tids[idx] = _tid_word;
}

RC
Cell_ic3::try_lock() {
  uint64_t v = _tid_word;
  if (v & LOCK_BIT) // already locked
    return false;
  return __sync_bool_compare_and_swap(&_tid_word, v, (v | LOCK_BIT));
}

void
Cell_ic3::add_to_acclist(txn_man * txn, TsType type) {
  // get new lock entry
  LockEntry * new_entry = (LockEntry *) mem_allocator.alloc(sizeof(LockEntry),
      _row->get_part_id());
  new_entry->type = type;
  new_entry->txn_id = txn->get_txn_id();
  new_entry->txn = txn;
  // add to tail
  LIST_PUT_TAIL(acclist, acclist_tail, new_entry);
}

LockEntry *
Cell_ic3::get_last_writer() {
  LockEntry * en = acclist_tail;
  while(en != NULL) {
    if (en->type == WR)
      break;
    en = en->prev;
  }
  return en;
}

LockEntry *
Cell_ic3::get_last_writer() {
  if (acclist_tail)
    return acclist_tail->txn;
  return NULL;
}

void
Cell_ic3::release() {
  assert(_tid_word & LOCK_BIT);
  _tid_word = _tid_word & (~LOCK_BIT);
}

void
Cell_ic3::rm_from_acclist(uint64_t txn_id) {
  LockEntry * en = acclist;
  while(en != NULL) {
    if (en->txn_id == txn_id)
      break;
    en = en->next;
  }
  if (en) {
    LIST_REMOVE_HT(en, acclist, acclist_tail);
  }
  return NULL;
}

//////////////////////// ROW_IC3 ////////////////////////
void
Row_ic3::init(row_t * row)
{
  _row = row;
  _tid_word = 0; // not used by cell-level lock
  cell_managers = (Cell_ic3 *) _mm_malloc(sizeof(Cell_ic3)
      *row->get_field_cnt, 64);
  for (int i = 0; i < row->get_field_cnt(); i++) {
    cell_managers[i].init(this, i);
  }
}

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
  txn->last_tid = _tid_word;
  return RCOK;
}

RC
Row_ic3::access(row_t * local_row, int idx, Access * txn_access) {
  return cell_managers[idx].access(local_row, txn_access);
}

RC
Row_ic3::try_lock(int idx) {
  return cell_managers[idx].try_lock();
}


///////////////////  fen ge xian ///////////////////

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

#endif
