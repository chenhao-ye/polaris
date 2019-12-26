cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=20
cnt=100000
penalty=1
wh=1

for alg in "CLV" "WOUND_WAIT" "WAIT_DIE" "NO_WAIT" 
do
for wh in 16 8 4 2 1
do
for cnt in 1000 100000
do
for threads in 32 16 8 4 2 1
do
	timeout 100 python test.py WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
done
done
done
done
