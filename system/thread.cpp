#include <algorithm>
#include <sched.h>
#include "global.h"
#include "manager.h"
#include "thread.h"
#include "txn.h"
#include "wl.h"
#include "query.h"
#include "plock.h"
#include "occ.h"
#include "vll.h"
#include "ycsb_query.h"
#include "tpcc_query.h"
#include "mem_alloc.h"
#include "test.h"

void thread_t::init(uint64_t thd_id, workload * workload) {
	_thd_id = thd_id;
	_wl = workload;
	srand48_r((_thd_id + 1) * get_sys_clock(), &buffer);
	_abort_buffer_size = ABORT_BUFFER_SIZE;
	_abort_buffer = (AbortBufferEntry *) _mm_malloc(sizeof(AbortBufferEntry) * _abort_buffer_size, 64);
	for (int i = 0; i < _abort_buffer_size; i++)
		_abort_buffer[i].query = NULL;
	_abort_buffer_empty_slots = _abort_buffer_size;
	_abort_buffer_enable = (g_params["abort_buffer_enable"] == "true");
}

uint64_t thread_t::get_thd_id() { return _thd_id; }
uint64_t thread_t::get_host_cid() {	return _host_cid; }
void thread_t::set_host_cid(uint64_t cid) { _host_cid = cid; }
uint64_t thread_t::get_cur_cid() { return _cur_cid; }
void thread_t::set_cur_cid(uint64_t cid) {_cur_cid = cid; }

RC thread_t::run() {
#if !NOGRAPHITE
	_thd_id = CarbonGetTileId();
#endif
	if (warmup_finish) {
		mem_allocator.register_thread(_thd_id);
	}
	pthread_barrier_wait( &warmup_bar );
	stats.init(get_thd_id());
	pthread_barrier_wait( &warmup_bar );

	set_affinity(get_thd_id());

	myrand rdm;
	rdm.init(get_thd_id());
	RC rc = RCOK;
	txn_man * m_txn;
	// get txn man from workload
	rc = _wl->get_txn_man(m_txn, this);
	assert (rc == RCOK);
	glob_manager->set_txn_man(m_txn);

	base_query * m_query = NULL;
	uint64_t thd_txn_id = 0;
	UInt64 txn_cnt = 0;
	ts_t txn_starttime = 0;
	uint64_t txn_backoff_time = 0;

	while (true) {
		ts_t starttime = get_sys_clock();
		if (WORKLOAD != TEST) {
			if (_abort_buffer_enable) {
                while(true) {
					m_query = NULL;
					ts_t curr_time = get_sys_clock();
					ts_t min_ready_time = UINT64_MAX;
					if (_abort_buffer_empty_slots < _abort_buffer_size) {
						for (int i = 0; i < _abort_buffer_size; i++) {
							if (_abort_buffer[i].query != NULL && curr_time > _abort_buffer[i].ready_time) {
								m_query = _abort_buffer[i].query;
                                m_query->rerun = true;
								txn_starttime = _abort_buffer[i].starttime;
								txn_backoff_time = _abort_buffer[i].backoff_time + (curr_time - _abort_buffer[i].abort_time);
								_abort_buffer[i].query = NULL;
								_abort_buffer_empty_slots ++;
								break;
							} else if (_abort_buffer_empty_slots == 0
					          		&& _abort_buffer[i].ready_time < min_ready_time)
								min_ready_time = _abort_buffer[i].ready_time;
							}
				    }
					if (m_query == NULL && _abort_buffer_empty_slots == 0) {
						M_ASSERT(min_ready_time >= curr_time, "min_ready_time=%ld, curr_time=%ld\n", min_ready_time, curr_time);
						usleep((min_ready_time - curr_time)/1000); 
					} else if (m_query == NULL) {
						m_query = query_queue->get_next_query( _thd_id );
                        m_query->rerun = false;
                        m_txn->abort_cnt = 0;
						assert(m_query);
                        txn_starttime = starttime;
						txn_backoff_time = 0;
#if CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT && WW_STARV_FREE)
						m_txn->set_ts(get_next_ts());
#endif
					}
					if (m_query)
						break;
				}
			} else {
				if (rc == RCOK) {
					m_query = query_queue->get_next_query( _thd_id );
                    m_query->rerun = false;
		            m_txn->abort_cnt = 0;
					assert(m_query);
                    txn_starttime = starttime;
					txn_backoff_time = 0;
#if CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT && WW_STARV_FREE)
					m_txn->set_ts(get_next_ts());
#endif
                }
			}
		}
		INC_STATS(_thd_id, time_query, get_sys_clock() - starttime);
//#if CC_ALG == VLL
//		_wl->get_txn_man(m_txn, this);
//#endif

#if (CC_ALG == WOUND_WAIT) && !WW_STARV_FREE
		m_txn->set_ts(get_next_ts());
#elif (CC_ALG == BAMBOO)
		m_txn->set_ts(0);
#elif CC_ALG == WAIT_DIE || (CC_ALG == WOUND_WAIT && WW_STARV_FREE)
        // used for after warmup, since aborted txn keeps original ts
        if (unlikely(m_txn->get_ts() == 0))
            m_txn->set_ts(get_next_ts());
#endif

#if CC_ALG == SILO_PRIO
#if SILO_PRIO_FIXED_PRIO
		m_txn->prio = m_query->prio;
#else
		m_txn->prio = std::min<int>(SILO_PRIO_MAX_PRIO,
			m_query->prio + (m_query->num_abort / SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT));
#endif // SILO_PRIO_FIXED_PRIO
#else // CC_ALG == SILO_PRIO
		m_txn->prio = m_query->prio;
#endif // CC_ALG == SILO_PRIO

		m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
		thd_txn_id ++;

		if ((CC_ALG == HSTORE && !HSTORE_LOCAL_TS)
			|| CC_ALG == MVCC
			|| CC_ALG == HEKATON
			|| CC_ALG == TIMESTAMP)
			m_txn->set_ts(get_next_ts());

		rc = RCOK;
#if CC_ALG == HSTORE
		if (WORKLOAD == TEST) {
			uint64_t part_to_access[1] = {0};
			rc = part_lock_man.lock(m_txn, &part_to_access[0], 1);
		} else
			rc = part_lock_man.lock(m_txn, m_query->part_to_access, m_query->part_num);
#elif CC_ALG == VLL
		vll_man.vllMainLoop(m_txn, m_query);
#elif CC_ALG == MVCC || CC_ALG == HEKATON
		glob_manager->add_ts(get_thd_id(), m_txn->get_ts());
#elif CC_ALG == OCC
		// In the original OCC paper, start_ts only reads the current ts without advancing it.
		// But we advance the global ts here to simplify the implementation. However, the final
		// results should be the same.
		m_txn->start_ts = get_next_ts();
#endif
		if (rc == RCOK)
		{
#if CC_ALG != VLL
			if (WORKLOAD == TEST)
				rc = runTest(m_txn);
			else {
			    rc = m_txn->run_txn(m_query);
			}
#endif
#if CC_ALG == HSTORE
			if (WORKLOAD == TEST) {
				uint64_t part_to_access[1] = {0};
				part_lock_man.unlock(m_txn, &part_to_access[0], 1);
			} else
				part_lock_man.unlock(m_txn, m_query->part_to_access, m_query->part_num);
#endif
		}

		ts_t endtime = get_sys_clock();

		if (rc == Abort) {
			++(m_query->num_abort);
			uint64_t penalty = 0;
			if (ABORT_PENALTY != 0)  {
				double r;
				drand48_r(&buffer, &r);
				penalty = r * ABORT_PENALTY;
			}
			if (!_abort_buffer_enable)
				usleep(penalty / 1000);
			else {
				assert(_abort_buffer_empty_slots > 0);
				for (int i = 0; i < _abort_buffer_size; i ++) {
					if (_abort_buffer[i].query == NULL) {
						_abort_buffer[i].query = m_query;
						_abort_buffer[i].abort_time = get_sys_clock();
						_abort_buffer[i].ready_time = _abort_buffer[i].abort_time + penalty;
						_abort_buffer[i].starttime = txn_starttime;
						_abort_buffer[i].backoff_time = txn_backoff_time;
						_abort_buffer_empty_slots --;
						break;
					}
				}
			}
		}

		// this is the time of the last execution
		uint64_t timespan = endtime - starttime;
		// this is the time of the whole txn
		uint64_t txn_timespan = endtime - txn_starttime;
		INC_STATS(get_thd_id(), run_time, timespan);
		//stats.add_lat(get_thd_id(), timespan);
		if (rc == RCOK) {
			INC_STATS(get_thd_id(), commit_latency, timespan);
			INC_STATS(get_thd_id(), latency, txn_timespan);
			INC_STATS(get_thd_id(), txn_cnt, 1);
#if WORKLOAD == YCSB
            if (unlikely(g_long_txn_ratio > 0)) {
                if ( ((ycsb_query *) m_query)->request_cnt > REQ_PER_QUERY)
                    INC_STATS(get_thd_id(), txn_cnt_long, 1);
            }
#endif
			ADD_PER_PRIO_STATS(get_thd_id(), exec_time, m_txn->prio, timespan);
			ADD_PER_PRIO_STATS(get_thd_id(), backoff_time, m_txn->prio, txn_backoff_time);
			ADD_PER_PRIO_STATS(get_thd_id(), txn_cnt, m_txn->prio, 1);
			ADD_PER_PRIO_STATS(get_thd_id(), abort_cnt, m_txn->prio, m_query->num_abort);
#if WORKLOAD == YCSB
			stats._stats[get_thd_id()]->append_latency(
				((ycsb_query *) m_query)->is_long, m_query->num_abort,
				m_txn->prio, txn_timespan);
#else
			stats._stats[get_thd_id()]->append_latency(
				false, m_query->num_abort, m_txn->prio, txn_timespan);
#endif
			stats.commit(get_thd_id());
			txn_cnt ++;
		} else if (rc == Abort) {
			INC_STATS(get_thd_id(), time_abort, timespan);
			INC_STATS(get_thd_id(), abort_cnt, 1);
#if WORKLOAD == YCSB
            if (unlikely(g_long_txn_ratio > 0)) {
                if ( ((ycsb_query *) m_query)->request_cnt > REQ_PER_QUERY)
                    INC_STATS(get_thd_id(), abort_cnt_long, 1);
            }
#endif
			ADD_PER_PRIO_STATS(get_thd_id(), abort_time, m_txn->prio, timespan);
			stats.abort(get_thd_id());
			m_txn->abort_cnt++;
		} else if (rc == ERROR) {
		    // user initiated aborts
		    INC_STATS(get_thd_id(), time_abort, timespan);
            INC_STATS(get_thd_id(), user_abort_cnt, 1);
            INC_STATS(get_thd_id(), abort_cnt, 1);
#if WORKLOAD == YCSB
            if (unlikely(g_long_txn_ratio > 0)) {
                if ( ((ycsb_query *) m_query)->request_cnt > REQ_PER_QUERY)
                    INC_STATS(get_thd_id(), abort_cnt_long, 1);
            }
#endif
            ADD_PER_PRIO_STATS(get_thd_id(), abort_time, m_txn->prio, timespan);
            stats.abort(get_thd_id());
            m_txn->abort_cnt ++;
		}

		if (rc == FINISH) {
#if CC_ALG == IC3
		    m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
#endif
			return rc;
		}
		if (!warmup_finish && txn_cnt >= WARMUP / g_thread_cnt)
		{
			stats.clear( get_thd_id() );
			return FINISH;
		}

#if TERMINATE_BY_COUNT
		if (warmup_finish && txn_cnt >= MAX_TXN_PER_PART) {
			assert(txn_cnt == MAX_TXN_PER_PART);
			if( !ATOM_CAS(_wl->sim_done, false, true) )
				assert( _wl->sim_done);
		}
#else
		if (warmup_finish && (stats._stats[get_thd_id()]->run_time / 1000000000 >=
		MAX_RUNTIME)) {
            if( !ATOM_CAS(_wl->sim_done, false, true) )
                assert( _wl->sim_done);
		}
#endif

		if (_wl->sim_done) {
#if CC_ALG == IC3
		    m_txn->set_txn_id(get_thd_id() + thd_txn_id * g_thread_cnt);
#endif
			return FINISH;
		}
	}
	assert(false);
}


ts_t
thread_t::get_next_n_ts(int n) {
	return glob_manager->get_n_ts(n);
}

ts_t
thread_t::get_next_ts() {
	if (g_ts_batch_alloc) {
		if (_curr_ts % g_ts_batch_num == 0) {
			_curr_ts = glob_manager->get_ts(get_thd_id());
			_curr_ts ++;
		} else {
			_curr_ts ++;
		}
		return _curr_ts - 1;
	} else {
		_curr_ts = glob_manager->get_ts(get_thd_id());
		return _curr_ts;
	}
}

RC thread_t::runTest(txn_man * txn)
{
	RC rc = RCOK;
	if (g_test_case == READ_WRITE) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
#if CC_ALG == OCC
		txn->start_ts = get_next_ts();
#endif
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 1);
		printf("READ_WRITE TEST PASSED\n");
		return FINISH;
	}
	else if (g_test_case == CONFLICT) {
		rc = ((TestTxnMan *)txn)->run_txn(g_test_case, 0);
		if (rc == RCOK)
			return FINISH;
		else
			return rc;
	}
	assert(false);
	return RCOK;
}
