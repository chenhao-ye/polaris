cd ../
cp -r config_ycsb_synthetic.h config.h

# algorithm
alg=CLV
spin="true"
# [WW]
ww_starv_free="false"
# [CLV]
dynamic="true"
on=1
retire_read="false"

# workload
wl="YCSB"
req=64
synthetic=false #true
zipf=0.9 #0
num_hs=0 #2
pos=SPECIFIED
specified=0.9
fixed=1
fhs="WR"
shs="WR"
read_ratio=0.1 #.9 #1
ordered="false"
flip=0
table_size="1024*1024*10"

# other
threads=32
profile="true"
cnt=100000 
penalty=50000

timeout 500 python test_debug.py RETIRE_READ=${retire_read} FIXED_HS=${fixed} FLIP_RATIO=${flip} SPECIFIED_RATIO=${specified} WW_STARV_FREE=${ww_starv_free} KEY_ORDER=$ordered READ_PERC=${read_ratio} NUM_HS=${num_hs} FIRST_HS=$fhs POS_HS=$pos DEBUG_TMP="false" DYNAMIC_TS=$dynamic CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic 
