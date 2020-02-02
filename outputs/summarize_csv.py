import pandas as pd
import sys
print(sys.argv[1])
df = pd.read_csv(sys.argv[1])
#summarized = df.groupby(['ABORT_PENALTY', 'CC_ALG', 'CLV_RETIRE_OFF', 'CLV_RETIRE_ON','DEBUG_BENCHMARK', 'DEBUG_CLV', 'DEBUG_PROFILING', 'DEBUG_TMP', 'DYNAMIC_TS', 'MAX_TXN_PER_PART', 'MERGE_HS', 'NUM_WH', 'PERC_PAYMENT', 'PRIORITIZE_HS', 'REORDER_WH', 'SPINLOCK', 'THREAD_CNT', 'WORKLOAD'])['abort_cnt', 'cycle_detect', 'deadlock_cnt', 'debug1', 'debug10','debug2', 'debug3', 'debug4', 'debug5', 'debug6', 'debug7', 'debug8','debug9', 'dl_detect_time', 'dl_wait_time', 'latency', 'run_time','throughput', 'time_abort', 'time_cleanup', 'time_index', 'time_man','time_query', 'time_ts_alloc', 'time_wait', 'txn_cnt'].mean()
summarized = df.groupby(['ABORT_PENALTY', 'CC_ALG', 'CLV_RETIRE_ON', 'DEBUG_BENCHMARK',
       'DEBUG_CLV', 'DEBUG_PROFILING', 'DEBUG_TMP', 'DYNAMIC_TS',
       'MAX_TXN_PER_PART', 'NUM_WH', 'PERC_PAYMENT', 'REORDER_WH', 'RETIRE_ON',
       'SPINLOCK', 'THREAD_CNT', 'WORKLOAD'])['abort_cnt', 'cycle_detect', 'deadlock_cnt', 'debug1', 'debug10','debug2', 'debug3', 'debug4', 'debug5', 'debug6', 'debug7', 'debug8','debug9', 'dl_detect_time', 'dl_wait_time', 'latency', 'run_time','throughput', 'time_abort', 'time_cleanup', 'time_index', 'time_man','time_query', 'time_ts_alloc', 'time_wait', 'txn_cnt'].mean()
summarized = summarized.reset_index()
summarized.to_csv("summarized_"+sys.argv[1], index=False)

