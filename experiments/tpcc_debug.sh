cp -r config-tpcc-std.h config.h

## algorithm
alg=$1
spin="true"
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
cs_pf="false"
opt_raw="true"

## workload
wl="TPCC"
wh=1
perc=0.5 # payment percentage
user_abort="true"
#user_abort="false"

#other
threads=16 
#threads=1 
profile="true"
cnt=100000
penalty=50000 

python test_debug.py CC_ALG=$alg SPINLOCK=$spin WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc TPCC_USER_ABORT=${user_abort} THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
#
