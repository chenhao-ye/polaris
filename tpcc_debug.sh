cp -r config_tpcc_debug.h config.h
rm debug.out

wl="TPCC"
threads=4
cnt=10000
penalty=1
wh=4
pf="false"

alg="CLV"
for threads in 1 4 8 16
do
timeout 30 python test.py DEBUG_PROFILING=${pf} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh} |& tee -a debug.out
done


