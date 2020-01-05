cp -r config_tpcc_debug.h config.h
rm debug.out

wl="TPCC"
threads=8
cnt=10000
penalty=1
wh=4

alg="CLV"

timeout 5 python test.py WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh} |& tee -a debug.out
valgrind --tool=callgrind ./rundb

alg="WOUND_WAIT"
timeout 5 python test.py WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh} |& tee -a debug.out
valgrind --tool=callgrind ./rundb
