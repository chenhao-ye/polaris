cd ../
cp -r config-std.h config.h

fname="ycsb-10-long-10k-smalldata"
# algorithm
latch=LH_MCSLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire_on="true"
cs_pf="false"
opt_raw="true"
max_waiter=0
last_retire=0.15

# workload
wl="YCSB"
req=16
synthetic=false
zipf=0.9
num_hs=0
pos=TOP
specified=0
fixed=1
fhs="WR"
shs="WR"
read_ratio=1
ordered="false"
flip=0
table_size="10000000"
long_ratio=0.1
long_req=10000

# other
threads=16
profile="true"
byc="false"
runtime=10
cnt=1000000
penalty=50000
chain="false"

read_ratio=0.5
numa="true"
for alg in BAMBOO WOUND_WAIT SILO WAIT_DIE NO_WAIT 
do
for i in 0 1 2 
do
timeout 1900 python test.py TERMINIATE_BY_COUNT=${byc} MAX_RUNTIME=${runtime} UNSET_NUMA=${numa} CC_ALG=${alg} LATCH=${latch} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=${dynamic} RETIRE_ON=${retire_on} DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} LAST_RETIRE=${last_retire} LONG_TXN_RATIO=${long_ratio} MAX_ROW_PER_TXN=${long_req} WORKLOAD=${wl} REQ_PER_QUERY=$req SYNTHETIC_YCSB=$synthetic ZIPF_THETA=$zipf NUM_HS=${num_hs} POS_HS=$pos SPECIFIED_RATIO=${specified} FIXED_HS=${fixed} FIRST_HS=$fhs SECOND_HS=$shs READ_PERC=${read_ratio} KEY_ORDER=$ordered FLIP_RATIO=${flip} SYNTH_TABLE_SIZE=${table_size} THREAD_CNT=$threads DEBUG_PROFILING=$profile MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty DEBUG_ABORT_LENGTH=${chain}
done
done
#

cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
