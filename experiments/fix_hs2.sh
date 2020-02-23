cd ../
cp -r config_ycsb_synthetic.h config.h

# algorithm
alg=WOUND_WAIT
spin="true"
# [WW]
ww_starv_free="false"
# [CLV]
dynamic="true"
on=0
retire_read="false"

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

retire_off_opt="true"
for fixed in 1 0
do
for i in 0 1 2 3 4
do
for alg in CLV #WOUND_WAIT
do
for specified in 0 0.25 0.5 0.75 1
do
for threads in 16 #32
do
for req in 16 #32
do
timeout 60 python test.py CLV_RETIRE_OFF=${retire_off_opt} RETIRE_READ=${retire_read} FIXED_HS=${fixed} FLIP_RATIO=${flip} SPECIFIED_RATIO=${specified} WW_STARV_FREE=${ww_starv_free} KEY_ORDER=$ordered READ_PERC=${read_ratio} NUM_HS=${num_hs} FIRST_HS=$fhs POS_HS=$pos DEBUG_TMP="false" DYNAMIC_TS=$dynamic CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic 
done
done
done
done
done
done

python experiments/send_email.py fix_hs2
