#if CC_ALG == ARIA

#include <pthread.h>
#include "global.h"

// coordinate threads to agree on the batch and phrase.
class AriaCoord {
	// there is one leader (thread 0) and many followers (other threads)
	// there are THREAD_CNT ctrl_block, where the first one is leader_block and
	// the rest are follower_block
	// 
	// Workflow:
	//   0. followers spin on follower_block.leader_says_start
	//   1. the leader sets each follower's leader_says_start
	//   2. followers clear leader_says_start and start to execute/commit
	//   3. when a follower finishes, it increments leader_block.exec_done_counter
	//      by one and spins on its leader_says_start
	//   4. when the leader is done, it spins on it leader_block to have
	//      (exec_done_counter == THREAD_CNT - 1)
	//   5. the leader clears leader_block.exec_done_counter, sets each
	//      follower's leader_says_start, repeat step 2
	union ctrl_block_t {
		struct {
			std::atomic_uint64_t follower_done_counter;
		} leader_block;
		struct {
			std::atomic_uint64_t leader_says_start;
		} follower_block;
		char padding[64]; // cacheline padding

		ctrl_block_t(): { memset(padding, 0, 64); }
	};
	static_assert(sizeof(ctrl_block_t) == 64, "ctrl_block_t must be cacheline-aligned");

 private:
	static ctrl_block_t* ctrl_blocks[THREAD_CNT];

 public:
	AriaCoord() {};

	static void register_ctrl_block(uint64_t thd_id, ctrl_block_t* ctrl_block) {
		ctrl_blocks[thd_id] = ctrl_block;
		if (thd_id)
			while (!ctrl_blocks[0]) {
				PAUSE // wait for the leader registered
			}
	}

	static uint64_t follower_wait_for_start(uint64_t thd_id) {
		assert(thd_id);
		// leader saves a nonzero value into leader_says_start to trigger
		// we make this value as batch_id
		// this is not required for correctness, but make debugging easier
		uint64_t batch_id;
		ctrl_blocks[0].leader_block.follower_done_counter.fetch_add(
			std::memory_order_acq_rel, std::memory_order_acquire);
		
		while (!(batch_id = ctrl_blocks[thd_id].follower_block.leader_says_start \
			.load(std::memory_order_relaxed)))
		{
			PAUSE
		}
		ctrl_blocks[thd_id].follower_block.leader_says_start.save(0, \
			std::memory_order_release);
		return batch_id;
	}

	static void leader_wait_for_done(uint64_t batch_id) {
		assert(batch_id);
		while (ctrl_blocks[0].leader_block.follower_done_counter.load( \
		std::memory_order_relaxed) != THREAD_CNT - 1) {
			PAUSE
		}
		ctrl_blocks[0].leader_block.follower_done_counter.store(0, 
			std::memory_order_release);
		for (uint64_t i = 1; i < THREAD_CNT; ++i)
			ctrl_blocks[i].follower_block.leader_says_start.store(batch_id,
				std::memory_order_release);
	}
};

#endif
