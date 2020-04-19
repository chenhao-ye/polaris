cd ../
cp -r config_tpcc_debug.h config.h

## algorithm
alg=BAMBOO
spin="true"
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
# [BAMBOO] lock optimization
retire_read="false"
on=0
off=0

## workload
wl="TPCC"
wh=1
perc=0.5 # payment percentage
# synthetic conditions
reorder="false"
bench="false" # if true, turn all get row to read only
# optimizations
# merge="false" # no longer supported

#other
threads=8 
profile="true"
cnt=100000
penalty=50000 

timeout 30 python test_debug.py CLV_RETIRE_ON=$on RETIRE_ON=$retire REORDER_WH=$reorder PERC_PAYMENT=$perc DEBUG_BENCHMARK=$bench DYNAMIC_TS=$dynamic DEBUG_PROFILING=${profile} SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh} RETIRE_READ=${retire_read} WW_STARV_FREE=${ww_starv_free}

