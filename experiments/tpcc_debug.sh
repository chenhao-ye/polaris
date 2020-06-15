cp -r config-tpcc-std.h config.h

## algorithm
alg=$1
latch=LH_SPINLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
cs_pf="true"
opt_raw="true"
max_waiter=0

## workload
wl="TPCC"
wh=16
perc=0.5 # payment percentage
user_abort="true"
com="false"
com_latch="false"

#other
threads=16 
profile="true"
cnt=100000
penalty=50000 

# temp
user_abort="false"
latch=LH_MCSLOCK
threads=16
wh=16
retire="false"
#dynamic="false"
#cnt=1000000

python test_debug.py CC_ALG=$alg LATCH=$latch WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc TPCC_USER_ABORT=${user_abort} COMMUTATIVE_OPS=$com COMMUTATIVE_LATCH=${com_latch} THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
#
