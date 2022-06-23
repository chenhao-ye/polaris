#pragma once

#if CC_ALG == ARIA

class base_query;

// coordinate threads to agree on the batch and phrase.
namespace AriaCoord {

void register_thread(uint64_t thd_id);
void unregister_thread(uint64_t thd_id);
bool start_exec_phase(uint64_t thd_id, uint64_t batch_id, bool sim_done);
void start_commit_phase(uint64_t thd_id, uint64_t batch_id);

} // namespace AriaCoord

#endif
