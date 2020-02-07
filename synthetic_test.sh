cp -r config_ycsb_synthetic.h config.h
rm debug.out

wl="YCSB"
alg="CLV"
threads=16
cnt=100000
penalty=0
zipf=0
synthetic="true"
table_size="1024*1024*20"
profile="true"
req=16
spin="true"
on=1
dynamic="true"
read_ratio=1
phs="true"
phs="false"

hs=1
#pos="TOP"
#pos="MID"
#pos="BOT"
#hs=2
#pos="TM"
fhs="WR"
shs="WR"
think=0

for i in 0 1
do
for penalty in 50000
do
for threads in 1 2 4 8 16
do
for pos in TOP MID BOT #TM MB
do
for alg in NO_WAIT #CLV SILO WOUND_WAIT WAIT_DIE
do
timeout 50 python test.py THINKTIME=$think PRIORITIZE_HS=$phs READ_PERC=1 NUM_HS=$hs FIRST_HS=$fhs POS_HS=$pos DEBUG_TMP="false" DYNAMIC_TS=$dynamic CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
done
done
done
done
done

#timeout 30 python test.py PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on SPINLOCK=$spin REQ_PER_QUERY=$req DEBUG_PROFILING=$profile SYNTH_TABLE_SIZE=${table_size} WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty ZIPF_THETA=$zipf SYNTHETIC_YCSB=$synthetic  |& tee -a debug.out
