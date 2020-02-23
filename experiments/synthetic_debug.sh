cd ../
cp -r config_ycsb_synthetic.h config.h

# algorithm
alg=CLV
#alg=WOUND_WAIT
spin="true"
# [WW]
ww_starv_free="false"
# [CLV]
dynamic="true"
on=0
retire_read="false"
retire="true"
retire_off_opt="false"
#retire="false"

# workload
wl="YCSB"
req=16
synthetic=true
zipf=0
num_hs=1
pos=SPECIFIED
specified=1
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

timeout 50 python test_debug.py CLV_RETIRE_OFF=${retire_off_opt} RETIRE_ON=${retire} RETIRE_READ=${retire_read} FIXED_HS=${fixed} FLIP_RATIO=${flip} SPECIFIED_RATIO=${specified} WW_STARV_FREE=${ww_starv_free} KEY_ORDER=$ordered READ_PERC=${read_ratio} NUM_HS=${num_hs} FIRST_HS=$fhs POS_HS=$pos DEBUG_TMP="false" DYNAMIC_TS=$dynamic CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic 
