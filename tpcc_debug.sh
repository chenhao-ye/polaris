cp -r config_tpcc_debug.h config.h
rm debug.out

wl="TPCC"
threads=16
cnt=100000
penalty=1
wh=1
spin="true"
pf="true"
alg="CLV"
on=0
off=17
phs="true"
phs="false"

#PRIORITIZE_HS=$phs

timeout 50 python test.py PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
