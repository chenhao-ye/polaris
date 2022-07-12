#include "global.h"
#include "txn.h"
#include "row.h"
#include "row_aria.h"

#if CC_ALG == ARIA

void 
Row_aria::init(row_t * row) {
	_row = row;
	_write_resv.store(0, std::memory_order_relaxed);
#if ARIA_REORDER
	_read_resv.store(0, std::memory_order_relaxed);
#endif
}

RC
Row_aria::access(txn_man * txn, TsType type, row_t * local_row) {
	if (type != R_REQ) {
		if (!reserve_write(txn->batch_id, txn->prio, txn->get_txn_id()))
			return Abort;
	}
#if ARIA_REORDER
	else {
		reserve_read(txn->batch_id, txn->prio, txn->get_txn_id());
	}
#endif
	// when in execution phase, everything is read-only except TID, so it is safe
	// to copy record data without any lock
#if ARIA_NOCOPY_READ
	// no need to make a copy because the whole database is read-only
	if (type == R_REQ) return RCOK;
#endif
	local_row->copy(_row);
	return RCOK;
}

void
Row_aria::write(row_t * data) {
	_row->copy(data);
}

#endif
