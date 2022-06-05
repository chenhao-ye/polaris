#include "global.h"
#include "txn.h"
#include "row.h"
#include "row_aria.h"

#if CC_ALG == ARIA

void 
Row_aria::init(row_t * row) {
	_row = row;
	_tid_word.store(0, std::memory_order_relaxed);
}

RC
Row_aria::access(txn_man * txn, TsType type, row_t * local_row) {
	if (type != R_REQ) {
		if (!try_reserve(txn->aria_tid))
			return Abort;
	}
	// when in execution phase, everything is read-only except TID, so it is safe
	// to copy record data without any lock
	local_row->copy(_row);
	return RCOK;
}

void
Row_aria::write(row_t * data) {
	_row->copy(data);
}

#endif
