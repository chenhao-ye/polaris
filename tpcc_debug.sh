cp -r config_tpcc_debug.h config.h
rm debug.out

wl="TPCC"
threads=2
cnt=1000
penalty=1
wh=1
alg="CLV"

timeout 60 python test.py WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh} |& tee -a debug.out
