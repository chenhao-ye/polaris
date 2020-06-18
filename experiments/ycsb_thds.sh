cd ../
cp -r config-ycsb-std.h config.h

fname="ycsb-thds"
# algorithm
alg=WOUND_WAIT
latch=LH_MCSLOCK
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire_on="true"
cs_pf="false"
opt_raw="true"
max_waiter=0

# workload
wl="YCSB"
req=16
synthetic=false
zipf=0
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

# other
threads=16
profile="true"
cnt=100000
penalty=50000
chain="false"

read_ratio=0.5
zipf=0.9
for latch in LH_MCSLOCK LH_SPINLOCK
do
for i in 0 1 2 3 4 5
do
for alg in BAMBOO WOUND_WAIT SILO WAIT_DIE NO_WAIT
do
for threads in 1 2 4 8 16 32
do
if [ $alg == "BAMBOO" ] 
then

for dynamic in true false
do
timeout 200 python test.py CC_ALG=${alg} LATCH=${latch} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=${dynamic} RETIRE_ON=${retire_on} DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} WORKLOAD=${wl} REQ_PER_QUERY=$req SYNTHETIC_YCSB=$synthetic ZIPF_THETA=$zipf NUM_HS=${num_hs} POS_HS=$pos SPECIFIED_RATIO=${specified} FIXED_HS=${fixed} FIRST_HS=$fhs SECOND_HS=$shs READ_PERC=${read_ratio} KEY_ORDER=$ordered FLIP_RATIO=${flip} SYNTH_TABLE_SIZE=${table_size} THREAD_CNT=$threads DEBUG_PROFILING=$profile MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty DEBUG_ABORT_LENGTH=${chain}
done 

else 

timeout 200 python test.py CC_ALG=${alg} LATCH=${latch} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=${dynamic} RETIRE_ON=${retire_on} DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} WORKLOAD=${wl} REQ_PER_QUERY=$req SYNTHETIC_YCSB=$synthetic ZIPF_THETA=$zipf NUM_HS=${num_hs} POS_HS=$pos SPECIFIED_RATIO=${specified} FIXED_HS=${fixed} FIRST_HS=$fhs SECOND_HS=$shs READ_PERC=${read_ratio} KEY_ORDER=$ordered FLIP_RATIO=${flip} SYNTH_TABLE_SIZE=${table_size} THREAD_CNT=$threads DEBUG_PROFILING=$profile MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty DEBUG_ABORT_LENGTH=${chain}

fi
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

cd experiments
python3 send_email.py ${fname}
