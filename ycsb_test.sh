cp -r config_ycsb_debug.h config.h

wl="YCSB"
threads=20
cnt=10000 #0
penalty=1
zipf=0
synthetic="true"
table_size="1024*1024*20"
req=16
pf="false"
spin="true"

for alg in "CLV" #"WOUND_WAIT" "WAIT_DIE" "NO_WAIT" 
do
for zipf in 0 
do
for threads in 1 2 4 8 16 #32
do
	timeout 50 python test.py REQ_PER_QUERY=$req SPINLOCK=$spin DEBUG_PROFILING=$pf SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
done
done
done
