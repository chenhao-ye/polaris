#include "txn.h"
#include "row.h"
#include "row_aria.h"
#include "config.h"
#include "aria.h"

#if CC_ALG == ARIA

namespace AriaCoord {

#if ARIA_USE_PTHREAD_BARRIER

static pthread_barrier_t phase_barrier;
static char padding1[CL_SIZE];
static std::atomic_bool global_sim_done;
static char padding2[CL_SIZE];
#ifndef NDEBUG
static uint64_t global_batch_id;
static char padding3[CL_SIZE];
#endif

// also works as "re-init"
void init() {
	int ret = pthread_barrier_init(&phase_barrier, NULL, THREAD_CNT);
	assert(ret == 0);
	global_sim_done.store(false, std::memory_order_release);
#ifndef NDEBUG
	global_batch_id = 0;
#endif
}

// there is nothing to do if using pthread barrier
// but this function itself works as a barrier
void register_thread(uint64_t thd_id) { pthread_barrier_wait(&phase_barrier); }

bool start_exec_phase(uint64_t thd_id, uint64_t batch_id, bool sim_done) {
	if (sim_done) global_sim_done.store(true, std::memory_order_release);
#ifndef NDEBUG // only in debug mode we maintain global_batch_id
	if (thd_id == 0) global_batch_id = batch_id;
#endif
	pthread_barrier_wait(&phase_barrier);
	assert(global_batch_id == batch_id);
	return !global_sim_done.load(std::memory_order_acquire);
}

void start_commit_phase(uint64_t thd_id, uint64_t batch_id) {
	assert(global_batch_id == batch_id);
	assert(!global_sim_done.load(std::memory_order_acquire));
	pthread_barrier_wait(&phase_barrier);
}

#else // ARIA_USE_PTHREAD_BARRIER

constexpr uint64_t BATCH_ID_SIM_DONE = std::numeric_limits<uint64_t>::max();

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
		std::atomic_bool sim_done;
	} leader_block;
	struct {
		std::atomic_uint64_t leader_says_start;
	} follower_block;
	char padding[CL_SIZE]; // cacheline padding
};
static_assert(sizeof(ctrl_block_t) == CL_SIZE, "ctrl_block_t must be cacheline-aligned");

static ctrl_block_t* ctrl_blocks[THREAD_CNT];

// also works for re-init
void init() {
	for (int i = 0; i < THREAD_CNT; ++i) {
		_mm_free(ctrl_blocks[i]);
		ctrl_blocks[i] = nullptr;
	}
}

void register_thread(uint64_t thd_id) {
	assert(!ctrl_blocks[thd_id]);
	ctrl_block_t* ctrl_block = (ctrl_block_t*) _mm_malloc(sizeof(ctrl_block_t), CL_SIZE);
	memset(ctrl_block->padding, 0, sizeof(CL_SIZE));
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

uint64_t follower_wait_for_start(uint64_t thd_id, bool sim_done) {
	assert(thd_id != 0);
	assert(thd_id < THREAD_CNT);
	// leader saves a nonzero value into leader_says_start to trigger
	// we make this value as batch_id
	// this is not required for correctness, but make debugging easier
	uint64_t batch_id;
	auto& done_cnt = ctrl_blocks[0]->leader_block.follower_done_cnt;
	auto& leader_cmd = ctrl_blocks[thd_id]->follower_block.leader_says_start;
	
	if (sim_done)
		ctrl_blocks[0]->leader_block.sim_done.store(true, std::memory_order_release);
	done_cnt.fetch_add(1, std::memory_order_acq_rel);
	while (!(batch_id = leader_cmd.load(std::memory_order_relaxed)))
		PAUSE
	leader_cmd.store(0, std::memory_order_release);
	assert(!sim_done || batch_id == BATCH_ID_SIM_DONE);
	return batch_id;
}

uint64_t leader_wait_for_done(uint64_t batch_id) {
	assert(batch_id != 0);
	auto& done_cnt = ctrl_blocks[0]->leader_block.follower_done_cnt;

	while (done_cnt.load(std::memory_order_relaxed) != THREAD_CNT - 1)
		PAUSE

	if (ctrl_blocks[0]->leader_block.sim_done.load(std::memory_order_acquire))
		batch_id = BATCH_ID_SIM_DONE;

	done_cnt.store(0, std::memory_order_release);
	for (uint64_t i = 1; i < THREAD_CNT; ++i)
		ctrl_blocks[i]->follower_block.leader_says_start.store(batch_id,
			std::memory_order_release);
	return batch_id;
}

bool start_new_phase(uint64_t thd_id, uint64_t batch_id, bool sim_done=false) {
	if (sim_done) batch_id = BATCH_ID_SIM_DONE;
	if (thd_id == 0) { // leader
		batch_id = leader_wait_for_done(batch_id);
		if (batch_id == BATCH_ID_SIM_DONE) return false;
	} else {
		uint64_t leader_batch_id = follower_wait_for_start(thd_id, sim_done);
		if (leader_batch_id == BATCH_ID_SIM_DONE) return false;
		assert(leader_batch_id == batch_id);
	}
	return true;
}

bool start_exec_phase(uint64_t thd_id, uint64_t batch_id, bool sim_done) {
	return start_new_phase(thd_id, batch_id, sim_done);
}

void start_commit_phase(uint64_t thd_id, uint64_t batch_id) {
	bool ret = start_new_phase(thd_id, batch_id);
	assert(ret);
}

#endif // ARIA_USE_PTHREAD_BARRIER

} // namespace AriaCoord

RC
txn_man::validate_aria() {
	RC rc = RCOK;

#if ARIA_REORDER
	// first validate WAW
	for (int rid = 0; rid < row_cnt; rid++) {
		if (accesses[rid]->type == WR \
			&& !accesses[rid]->orig_row->manager->validate_write(batch_id, prio,
				get_txn_id()))
		{
			rc = Abort;
			goto final;
		}
	}

	// then validate RAW
	for (int rid = 0; rid < row_cnt; rid++) {
		if (accesses[rid]->type != WR \
			&& !accesses[rid]->orig_row->manager->validate_write(batch_id, prio,
				get_txn_id()))
		{
			rc = Abort;
			break;
		}
	}
	// if we pass RAW validation, we can just commit; if not, we must try reorder
	if (rc == RCOK) goto commit;

	// validate WAR to reorder
	for (int rid = 0; rid < row_cnt; rid++) {
		if (accesses[rid]->type == WR \
			&& !accesses[rid]->orig_row->manager->validate_read(batch_id, prio,
				get_txn_id()))
		{
			rc = Abort;
			break;
		}
	}
#else // !ARIA_REORDER
	for (int rid = 0; rid < row_cnt; rid++) {
		if (!accesses[rid]->orig_row->manager->validate_write(batch_id, prio,
			get_txn_id()))
		{
			rc = Abort;
			goto final;
		}
	}
#endif // ARIA_REORDER

commit:
	for (int rid = 0; rid < row_cnt; rid++)
		if (accesses[rid]->type == WR)
			accesses[rid]->orig_row->manager->write(accesses[rid]->data);

final:
	cleanup(rc);
	return rc;
}

#endif
