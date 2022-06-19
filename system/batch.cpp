
#include "batch.h"
#include "wl.h"

void BatchMgr::BatchBuffer::init_txn(workload* wl, thread_t* thd) {
	for (auto& e: batch) {
		RC rc = wl->get_txn_man(e.txn, thd);
		assert(rc == RCOK);
	}
}
