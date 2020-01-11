cp -r config_ycsb_debug.h config.h

wl="YCSB"
threads=20
cnt=10000 #00
penalty=1
zipf=0
synthetic="true"
table_size="1024*1024*20"
profile="true"

for alg in "CLV" "WOUND_WAIT" # "WAIT_DIE" "NO_WAIT" 
do
for zipf in 0
do
for threads in 1 2 4 8   # 2 4 8 16 32
do
	timeout 30 python test.py DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
done
done
done
