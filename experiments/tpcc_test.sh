cd ../
cp -r config-tpcc-std.h config.h

## algorithm
alg=BAMBOO
spin="true"
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire="true"
cs_pf="false"

## workload
wl="TPCC"
wh=2
perc=0.5 # payment percentage

#other
threads=8
profile="true"
cnt=100000
penalty=50000

for i in 0 1 2 3 4
do
for threads in 1 2 4 8 16 32
do
for alg in BAMBOO
do
timeout 30 python test.py CC_ALG=$alg SPINLOCK=$spin WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire DEBUG_CS_PROFILING=${cs_pf} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
done
done
done
#

cd outputs/
python3 collect_stats.py
mv stats.csv tpcc_bb.csv
mv stats.json tpcc_bb.json
cd ..

cd experiments/
python3 send_email.py tpcc
