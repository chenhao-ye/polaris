cd ../
rm outputs/stats.json
cp -r config-std.h config.h
fname="tpcc_ic3_payment"

## algorithm
alg=BAMBOO
latch=LH_MCSLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
cs_pf="false"
opt_raw="true"
max_waiter=0
# ic3
ic3_eager="true"
ic3_rd="true"
ic3_field="false"

## workload
wl="TPCC"
wh=2
perc=0.5 # payment percentage
user_abort="true"
com="false"

#other
threads=8
profile="true"
cnt=100000
penalty=50000

perc=1 # payment percentage
for i in 0 1 2 3 4 
do
for alg in IC3 BAMBOO SILO WOUND_WAIT WAIT_DIE NO_WAIT
do
for threads in 1 2 4 8 16 32
do
for wh in 1 2 4 8 16
do
ic3_eager="true"
ic3_rd="true"
timeout 200 python test.py CC_ALG=$alg LATCH=$latch WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} IC3_EAGER_EXEC=${ic3_eager} IC3_RENDEZVOUS=${ic3_rd} IC3_FIELD_LOCKING=${ic3_field} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc TPCC_USER_ABORT=${user_abort} COMMUTATIVE_OPS=$com THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
done
done
done
done
#

cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

cd experiments/
python3 send_email.py ${fname}
