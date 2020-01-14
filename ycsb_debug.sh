cp -r config_ycsb_debug.h config.h

wl="YCSB"
threads=3
cnt=1 #00 #0000 #00
penalty=1
zipf=0
synthetic="true"
table_size="1024*1024*10"
profile="true"
req=5
spin="true"
phs="true"
on=1

timeout 30 python test.py PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out

