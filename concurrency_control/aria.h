#pragma once

#if CC_ALG == ARIA

class base_query;

// coordinate threads to agree on the batch and phrase.
namespace AriaCoord {

void register_ctrl_block(uint64_t thd_id);
void start_new_phase(uint64_t thd_id, uint64_t batch_id);

} // namespace AriaCoord

#endif
