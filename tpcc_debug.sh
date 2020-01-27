cp -r config_tpcc_debug.h config.h
rm temp.out
rm debug.out

wl="TPCC"
threads=16
cnt=100000
penalty=0 #1
wh=1
spin="true"
pf="true"
alg="CLV"
on=1
off=17
phs="true"
phs="false"
dynamic="true"
dynamic="false"
debug="false"
#debug="true"
nodist="true"
nodist="false"
perc=0.5
#perc=0
#perc=1
merge="true"
merge="false"
reorder="false"
#reorder="true"
retire="true"
#retire="false"



for reorder in true 
do
for alg in CLV WOUND_WAIT #WAIT_DIE NO_WAIT
do
for threads in 16 #8 4 2 1
do
timeout 90 python test.py RETIRE_ON=$retire REORDER_WH=$reorder MERGE_HS=$merge PERC_PAYMENT=$perc DEBUG_BENCHMARK=$nodist DEBUG_CLV=$debug DYNAMIC_TS=$dynamic PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
done
done
done


#timeout 50 python test.py DEBUG_TMP="false" PRIORITIZE_HS=$phs CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
