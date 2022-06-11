#pragma once

#include "global.h"
#include "txn.h"

struct BatchEntry {
	base_query* query; // the current query to execute
	ts_t starttime; // if zero, meaning it is a newly start one
	RC rc; // current state; can be Abort if its reservation fails
	txn_man* txn; // init once and reused repeatedly
};

/*
 * Batch manager.
 * Currently only used by Aria. It manages txns batches by batches.
 */
class BatchMgr {
	struct BatchBuffer { // this should be a FIFO queue
		BatchEntry batch[ARIA_BATCH_SIZE] {};
		int size = 0;

		BatchBuffer() = default;
		void init_txn(workload* wl, thread_t* thd);
	
		void reset() { size = 0; }
		void append(base_query* q, ts_t t = 0) {
			assert(size < ARIA_BATCH_SIZE);
			batch[size] = {q, t, RCOK};
			++size;
		}
		void append(struct BatchEntry* other) {
			assert(size < ARIA_BATCH_SIZE);
			batch[size] = *other;
			++size;
		}
		BatchEntry* get(int idx) {
			if (idx >= size) return nullptr; // nothing to pop
			return &batch[idx];
		}
	};

	uint64_t batch_id;
	BatchBuffer batch_buf0;
	BatchBuffer batch_buf1;
	BatchBuffer* curr_batch;
	BatchBuffer* next_batch;

 public:
	BatchMgr(): batch_id(0), batch_buf0(), batch_buf1(), curr_batch(&batch_buf0), 
							next_batch(&batch_buf1) {}
	void init_txn(workload* wl, thread_t* thd) {
		batch_buf0.init_txn(wl, thd);
		batch_buf1.init_txn(wl, thd);
	}

	const uint64_t get_batch_id() const { return batch_id; }

	// get one entry from the current batch
	BatchEntry* get_entry(int idx) const { return curr_batch->get(idx); }
	// a txn aborted, put it into the next batch
	void put_next(BatchEntry* e) { next_batch->append(e); }

	// whether there is any space left on the current batch
	bool can_admit() { return curr_batch->size < ARIA_BATCH_SIZE; }
	// admit new query into the buffer
	void admit_new_query(base_query* q) { 
		assert(curr_has_space);
		curr_batch->append(q);
	}

	// start a new batch:
	// next_batch becomes "curr_batch"; recycle the old one as new "next_batch"
	void start_new_batch() {
		++batch_id; // batch_id must be nonzero
		// switch curr/next_batch
		BatchBuffer* tmp = curr_batch;
		curr_batch = next_batch;
		next_batch = tmp;
		next_batch->reset();
	}
};
