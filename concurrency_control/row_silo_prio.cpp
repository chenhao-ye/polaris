#include "global.h"
#include "txn.h"
#include "row.h"
#include "row_silo_prio.h"
#include "mem_alloc.h"
#include <atomic>

#if CC_ALG == SILO_PRIO

void 
Row_silo_prio::init(row_t * row) 
{
	_row = row;
	_tid_word.store({0, 0}, std::memory_order_relaxed);
}

RC
Row_silo_prio::access(txn_man * txn, TsType type, row_t * local_row) {
	TID_prio_t v, v2;
	const uint32_t prio = txn->prio;
	bool is_reserved;
	v = _tid_word.load(std::memory_order_relaxed);
retry:
	while (v.is_locked()) {
		PAUSE
		v = _tid_word.load(std::memory_order_relaxed);
	}
	// for a write, abort if the current priority is higher
	if (prio < v.get_prio()) {
		if (type != R_REQ) return Abort;
	}
	v2 = v;
	is_reserved = v2.acquire_prio(prio);
	local_row->copy(_row);
	if (!_tid_word.compare_exchange_strong(v, v2, std::memory_order_acq_rel,
		std::memory_order_acquire))
		goto retry;
	txn->last_is_owner = is_reserved;
	txn->last_data_ver = v2.get_data_ver();
	if (is_reserved) txn->last_prio_ver = v2.get_prio_ver();
	return RCOK;
}

void Row_silo_prio::write(row_t * data) {
	_row->copy(data);
}

#endif
