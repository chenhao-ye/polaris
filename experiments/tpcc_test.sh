cd ../
cp -r config_tpcc_debug.h config.h

## algorithm
alg=BAMBOO
spin="true"
# [WW]
ww_starv_free="false"
# [BAMBOO]
dynamic="true"
on=0
retire_read="false"
retire="true"
# [BAMBOO] lock optimization
# delay=0
# dt=4

## workload
wl="TPCC"
wh=1
perc=0.5 # payment percentage
# synthetic conditions
reorder="false"
bench="false" # if true, turn all get row to read only
# optimizations
# merge="false" # no longer supported

#other
threads=16 
profile="true"
cnt=100000
penalty=50000 

retire_off_opt="true"
for retire_off_opt in true false
do
for alg in BAMBOO #WOUND_WAIT SILO WAIT_DIE NO_WAIT
do
for i in 0 1 2 3 4
do
for threads in 1 2 4 8 16 32
do
for wh in 1 2 4 8 16
do
timeout 200 python test.py CLV_RETIRE_OFF=${retire_off_opt} CLV_RETIRE_ON=$on RETIRE_ON=$retire REORDER_WH=$reorder PERC_PAYMENT=$perc DEBUG_BENCHMARK=$bench DYNAMIC_TS=$dynamic DEBUG_PROFILING=${profile} SPINLOCK=$spin WORKLOAD=${wl} CC_ALG=$alg THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty NUM_WH=${wh} RETIRE_READ=${retire_read} WW_STARV_FREE=${ww_starv_free}
done
done
done
done
done

cd outputs/
python3 collect_stats.py
mv stats.csv tpcc/tpcc.csv
mv stats.json tpcc_clv.json
cd ..

cd experiments/
python3 send_email.py tpcc-node2
