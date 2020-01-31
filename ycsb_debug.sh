cp -r config_ycsb_debug.h config.h
rm debug.out

wl="YCSB"
alg="CLV"
alg="SILO"
threads=16
cnt=100000
penalty=0
zipf=0
synthetic="true"
table_size="1024*1024*20"
profile="true"
req=64
spin="true"
phs="false"
on=1
dynamic="true"

for alg in CLV SILO
do
timeout 30 python test.py DEBUG_TMP="false" DYNAMIC_TS=$dynamic PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
done
#phs="false"

#timeout 30 python test.py PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
