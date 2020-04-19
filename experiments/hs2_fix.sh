cd ../
cp -r config-ycsb-synthetic-std.h config.h

# algorithm
alg=WOUND_WAIT
spin="true"
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
retire_on="true"
cs_pf="false"

# workload
wl="YCSB"
req=16
synthetic=true
zipf=0
num_hs=2
pos=SPECIFIED
specified=0.9
fixed=1
fhs="WR"
shs="WR"
read_ratio=1
ordered="false"
flip=0
table_size="1024*1024*20"

# other
threads=16
profile="true"
cnt=100000 
penalty=50000

for fixed in 1 0
do
for i in 0 1 2 3 4
do
for alg in BAMBOO #WOUND_WAIT
do
for specified in 0 0.25 0.5 0.75 1
do
for threads in 16 #32
do
for req in 16 #32
do
timeout 50 python test.py CC_ALG=${alg} SPINLOCK=${spin} WW_STARV_FREE=${ww_starv_free} DYNAMIC_TS=${dynamic} RETIRE_ON=${retire_on} DEBUG_CS_PROFILING=${cs_pf} WORKLOAD=${wl} REQ_PER_QUERY=$req SYNTHETIC_YCSB=$synthetic ZIPF_THETA=$zipf NUM_HS=${num_hs} POS_HS=$pos SPECIFIED_RATIO=${specified} FIXED_HS=${fixed} FIRST_HS=$fhs SECOND_HS=$shs READ_PERC=${read_ratio} KEY_ORDER=$ordered FLIP_RATIO=${flip} SYNTH_TABLE_SIZE=${table_size} THREAD_CNT=$threads DEBUG_PROFILING=$profile MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty
done
done
done
done
done
done

cd outputs/
python3 collect_stats.py
mv stats.csv hs2.csv
mv stats.json hs2.json
rm stats.json
cd ..

python experiments/send_email.py fix_hs2
