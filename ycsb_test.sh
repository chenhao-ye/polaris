cp -r config_ycsb_debug.h config.h

wl="YCSB"
threads=20
cnt=100000
penalty=1
zipf="0.6"
synthetic="true"

for alg in "CLV" "WOUND_WAIT" "WAIT_DIE" "NO_WAIT" 
do
for zipf in 0.6 0.9
do
for threads in 1 2 4 8 16 32
do
	timeout 100 python test.py WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
done
done
done
