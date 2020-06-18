cd ../
cp -r config-ycsb-synthetic-std.h config.h

fname="hs1_top_req"

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
synthetic=true
zipf=0
num_hs=1
pos=TOP
specified=0
fixed=1
fhs="WR"
shs="WR"
read_ratio=1
ordered="false"
flip=0
table_size="10000000"
chain="false"

# other
threads=16
profile="true"
cnt=100000 
penalty=50000

# figure 4: normalized throughput with optimal case, varying requests
for latch in LH_MCSLOCK LH_SPINLOCK
do
for i in 0 1 2 3 4
do
for alg in WOUND_WAIT BAMBOO
do
for threads in 1 2 4 8 16 32
do
for req in 4 16 64
do
timeout 1000 python test.py CC_ALG=${alg} LATCH=${latch} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=${dynamic} RETIRE_ON=${retire_on} DEBUG_CS_PROFILING=${cs_pf} BB_OPT_RAW=${opt_raw} BB_OPT_MAX_WAITER=${max_waiter} WORKLOAD=${wl} REQ_PER_QUERY=$req SYNTHETIC_YCSB=$synthetic ZIPF_THETA=$zipf NUM_HS=${num_hs} POS_HS=$pos SPECIFIED_RATIO=${specified} FIXED_HS=${fixed} FIRST_HS=$fhs SECOND_HS=$shs READ_PERC=${read_ratio} KEY_ORDER=$ordered FLIP_RATIO=${flip} SYNTH_TABLE_SIZE=${table_size} THREAD_CNT=$threads DEBUG_PROFILING=$profile MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty DEBUG_ABORT_LENGTH=$chain
done
done
done
done
done

cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

python experiments/send_email.py ${fname}
