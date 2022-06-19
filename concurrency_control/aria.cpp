#include "txn.h"
#include "row.h"
#include "row_aria.h"
#include "config.h"
#include "aria.h"

#if CC_ALG == ARIA

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
	if (thd_id == 0) {
		for (int i = 1; i < THREAD_CNT; ++i) {
			while (!ctrl_blocks[i]) PAUSE
		}
		// only register after all followers finish
		// this fires all threads to return from `register_ctrl_block`
		ctrl_blocks[0] = ctrl_block;
	} else {
		ctrl_blocks[thd_id] = ctrl_block;
		while (!ctrl_blocks[0]) PAUSE // wait for the leader registered
	}
}

uint64_t follower_wait_for_start(uint64_t thd_id) {
	assert(thd_id != 0);
	assert(thd_id < THREAD_CNT);
	// leader saves a nonzero value into leader_says_start to trigger
	// we make this value as batch_id
	// this is not required for correctness, but make debugging easier
	uint64_t batch_id;
	auto& done_cnt = ctrl_blocks[0]->leader_block.follower_done_cnt;
	auto& leader_cmd = ctrl_blocks[thd_id]->follower_block.leader_says_start;
	
	done_cnt.fetch_add(1, std::memory_order_acq_rel);
	while (!(batch_id = leader_cmd.load(std::memory_order_relaxed)))
		PAUSE
	leader_cmd.store(0, std::memory_order_release);
	return batch_id;
}

void leader_wait_for_done(uint64_t batch_id) {
	assert(batch_id != 0);
	auto& done_cnt = ctrl_blocks[0]->leader_block.follower_done_cnt;

	while (done_cnt.load(std::memory_order_relaxed) != THREAD_CNT - 1)
		PAUSE
	done_cnt.store(0, std::memory_order_release);
	for (uint64_t i = 1; i < THREAD_CNT; ++i)
		ctrl_blocks[i]->follower_block.leader_says_start.store(batch_id,
			std::memory_order_release);
}

void start_new_phase(uint64_t thd_id, uint64_t batch_id) {
	if (thd_id == 0) { // leader
		leader_wait_for_done(batch_id);
	} else { // followers
		uint64_t leader_batch_id = follower_wait_for_start(thd_id);
		assert(leader_batch_id == batch_id); // followers must match leader's
	}
}

}

RC
txn_man::validate_aria() {
	RC rc = RCOK;
	for (int rid = 0; rid < row_cnt; rid++) {
		if (!accesses[rid]->orig_row->manager->validate(batch_id, prio,
		                                                get_txn_id()))
		{
			rc = Abort;
			goto final;
		}
	}

	for (int rid = 0; rid < row_cnt; rid++)
		if (accesses[rid]->type == WR)
			accesses[rid]->orig_row->manager->write(accesses[rid]->data);

final:
	cleanup(rc);
	return rc;
}

#endif
