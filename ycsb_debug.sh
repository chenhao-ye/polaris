cp -r config_ycsb_debug.h config.h

wl="YCSB"
alg="CLV"
threads=4
cnt=10
penalty=1
zipf=0.9
synthetic="true"
table_size="100" #"1024*1024*20"
profile="true"
req=6
spin="true"
phs="true"
on=1

timeout 30 python test.py DEBUG_CLV="true" PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out

#phs="false"

#timeout 30 python test.py PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
