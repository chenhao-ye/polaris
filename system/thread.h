#pragma once

#include "global.h"

class workload;
class base_query;

class thread_t {
  public:
    uint64_t _thd_id;
    workload * _wl;

    uint64_t 	get_thd_id();

    uint64_t 	get_host_cid();
    void 	 	set_host_cid(uint64_t cid);

    uint64_t 	get_cur_cid();
    void 		set_cur_cid(uint64_t cid);

    void 		init(uint64_t thd_id, workload * workload);
    // the following function must be in the form void* (*)(void*)
    // to run with pthread.
    // conversion is done within the function.
    RC 			run();

    // moved from private to global for clv
    ts_t 		get_next_ts();
    ts_t 		get_next_n_ts(int n);


  private:
    uint64_t 	_host_cid;
    uint64_t 	_cur_cid;
    ts_t 		_curr_ts;

    RC	 		runTest(txn_man * txn);
    drand48_data buffer;

    // added for wound wait
    base_query * curr_query;
    ts_t         starttime;

    // A restart buffer for aborted txns.
    struct AbortBufferEntry	{
        ts_t abort_time; // abort_time + penalty == ready_time
        ts_t ready_time;
        base_query * query;
        ts_t starttime;
        uint64_t backoff_time; // accumulated backoff time
    };
    AbortBufferEntry * _abort_buffer;
    int _abort_buffer_size;
    int _abort_buffer_empty_slots;
    bool _abort_buffer_enable;
};
