#pragma once

#include "global.h"
#include "txn.h"

struct BatchEntry {
	txn_man* txn; // init once and reused repeatedly
	base_query* query; // the current query to execute
	RC rc; // current state; can be Abort if its reservation fails
	ts_t start_ts; // if zero, meaning it is a newly start one
	// the name is a little confusing: exec_time here includes validation phases
	uint64_t exec_time_curr; // current execution time (not yet abort/commit)
	uint64_t exec_time_abort; // how much execution time spent eventually aborts
	uint64_t txn_id;
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
		void append(base_query* q) {
			assert(size < ARIA_BATCH_SIZE);
			batch[size].query = q;
			batch[size].rc = RCOK;
			batch[size].start_ts = 0;
			batch[size].exec_time_curr = 0;
			batch[size].exec_time_abort = 0;
			batch[size].txn_id = 0;
			++size;
		}
		void append(struct BatchEntry* other) {
			assert(size < ARIA_BATCH_SIZE);
			batch[size].query = other->query;
			batch[size].rc = RCOK;
			batch[size].start_ts = other->start_ts;
			batch[size].exec_time_curr = 0;
			batch[size].exec_time_abort = other->exec_time_abort;
			batch[size].txn_id = other->txn_id;
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
		batch_buf0.reset();
		batch_buf1.reset();
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
		assert(q);
		assert(can_admit());
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
