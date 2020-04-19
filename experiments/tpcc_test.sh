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
wh=1
perc=0.5 # payment percentage

#other
threads=8
profile="true"
cnt=100000
penalty=50000

timeout 30 python test.py CC_ALG=$alg SPINLOCK=$spin
WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=$dynamic RETIRE_ON=$retire
DEBUG_CS_PROFILING=${cs_pf} WORKLOAD=${wl} NUM_WH=${wh} PERC_PAYMENT=$perc
THREAD_CNT=$threads DEBUG_PROFILING=${profile} MAX_TXN_PER_PART=$cnt
ABORT_PENALTY=$penalty

cd outputs/
python3 collect_stats.py
mv stats.csv tpcc.csv
mv stats.json tpcc.json
cd ..

cd experiments/
python3 send_email.py tpcc
