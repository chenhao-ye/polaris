#if CC_ALG == ARIA

#include <pthread.h>
#include "config.h"

class base_query;

// coordinate threads to agree on the batch and phrase.
namespace AriaCoord {
// there is one leader (thread 0) and many followers (other threads)
// there are THREAD_CNT ctrl_block, where the first one is leader_block and
// the rest are follower_block
// 
// Workflow:
//   0. followers spin on follower_block.leader_says_start
//   1. the leader sets each follower's leader_says_start
//   2. followers clear leader_says_start and start to execute/commit
//   3. when a follower finishes, it increments leader_block.exec_done_cnt
//      by one and spins on its leader_says_start
//   4. when the leader is done, it spins on it leader_block to have
//      (exec_done_cnt == THREAD_CNT - 1)
//   5. the leader clears leader_block.exec_done_cnt, sets each
//      follower's leader_says_start, repeat step 2
union ctrl_block_t {
	struct {
		std::atomic_uint64_t follower_done_cnt;
	} leader_block;
	struct {
		std::atomic_uint64_t leader_says_start;
	} follower_block;
	char padding[64]; // cacheline padding

	ctrl_block_t() { memset(padding, 0, 64); }
};
static_assert(sizeof(ctrl_block_t) == 64, "ctrl_block_t must be cacheline-aligned");

static ctrl_block_t* ctrl_blocks[THREAD_CNT];

void register_ctrl_block(uint64_t thd_id) {
	ctrl_block_t* ctrl_block = (ctrl_block_t*) _mm_malloc(sizeof(ctrl_block_t), 64);
	ctrl_blocks[thd_id] = ctrl_block;
	if (thd_id) while (!ctrl_blocks[0]) PAUSE // wait for the leader registered
}

uint64_t follower_wait_for_start(uint64_t thd_id) {
	assert(thd_id);
	// leader saves a nonzero value into leader_says_start to trigger
	// we make this value as batch_id
	// this is not required for correctness, but make debugging easier
	uint64_t batch_id;
	auto& done_cnt = ctrl_blocks[0]->leader_block.follower_done_cnt;
	auto& leader_cmd = ctrl_blocks[thd_id]->follower_block.leader_says_start;
	
	done_cnt.fetch_add(std::memory_order_acq_rel, std::memory_order_acquire);
	while (!(batch_id = leader_cmd.load(std::memory_order_relaxed)))
		PAUSE
	leader_cmd.store(0, std::memory_order_release);
	return batch_id;
}

void leader_wait_for_done(uint64_t batch_id) {
	assert(batch_id);
	auto& done_cnt = ctrl_blocks[0]->leader_block.follower_done_cnt;

	while (done_cnt.load(std::memory_order_relaxed) != THREAD_CNT - 1)
		PAUSE
	done_cnt.store(0, std::memory_order_release);
	for (uint64_t i = 1; i < THREAD_CNT; ++i)
		ctrl_blocks[i]->follower_block.leader_says_start.store(batch_id,
			std::memory_order_release);
}

void start_new_phase(uint64_t thd_id, uint64_t batch_id) {
	if (!thd_id) { // leader
		leader_wait_for_done(batch_id);
	} else { // followers
		uint64_t leader_batch_id = follower_wait_for_start(thd_id);
		assert(leader_batch_id == batch_id); // followers must match leader's
	}
}
} // namespace AriaCoord

class AriaBatchMgr {
	struct BatchEntry {
		base_query * query;
		ts_t starttime; // if zero, meaning it is a newly start one
		RC rc; // current state; can be Abort if its reservation fails
	};

	struct BatchBuffer { // this should be a FIFO queue
		BatchEntry batch[ARIA_BATCH_SIZE] {};
		int size = 0;

		BatchBuffer() = default;
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

	BatchBuffer batch_buf0;
	BatchBuffer batch_buf1;
	BatchBuffer* curr_batch;
	BatchBuffer* next_batch;

 public:
	AriaBatchMgr(): batch_buf0(), batch_buf1(), curr_batch(&batch_buf0), 
									next_batch(&batch_buf1) {}
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
		// switch curr/next_batch
		BatchBuffer* tmp = curr_batch;
		curr_batch = next_batch;
		next_batch = tmp;
		next_batch->reset();
	}
};

#endif
