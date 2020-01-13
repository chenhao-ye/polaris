cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=20
cnt=100000
penalty=1
wh=1
spin="true"
pf="true"
on=0
off=16


#for i in {1..3}
#do
for alg in "CLV" #"WOUND_WAIT" "WAIT_DIE" "NO_WAIT" 
do
for wh in 16 8 4 2 1
do
for cnt in 100000
do
for threads in 16 8 4 2 1
do
	timeout 100 python test.py CLV_RETIRE_ON=$on CLV_RETIRE_OFF=$off DEBUG_PROFILING=$pf SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh}|& tee -a debug.out
#done
done
done
done
done
