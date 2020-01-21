cp -r config_tpcc_debug.h config.h
rm debug.out

wl="TPCC"
threads=16
#threads=8
cnt=100000
penalty=1
wh=1
spin="true"
pf="true"
alg="CLV"
on=1
off=17
phs="true"
phs="false"
tmp="true"
tmp="false"
dynamic="true"
debug="false"
#debug="true"

timeout 50 python test.py DEBUG_CLV=$debug DYNAMIC_TS=$dynamic DEBUG_TMP=$tmp PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out

#timeout 50 python test.py DEBUG_TMP="false" PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
