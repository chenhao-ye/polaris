cd ../
cp -r config-tpcc-std.h config.h

## algorithm
alg=WOUND_WAIT
latch=LH_SPINLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
cs_pf="true"
opt_raw="false"

## workload
wl="TPCC"
wh=2
perc=0.5 # payment percentage
user_abort="true"

#other
threads=8
profile="true"
cnt=100000
penalty=50000


for i in 0 1 2 3 4
do
for user_abort in true false
do
for threads in 8 16 32
do
for wh in 1 2 4
do
timeout 100 python test.py CC_ALG=$alg LATCH=${latch} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc TPCC_USER_ABORT=${user_abort} THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
done
done
done
done
#

cd outputs/
python3 collect_stats.py
mv stats.csv tpcc_ww_pf.csv
mv stats.json tpcc_ww_pf.json
cd ..

cd experiments/
python3 send_email.py tpcc_bb_pf
