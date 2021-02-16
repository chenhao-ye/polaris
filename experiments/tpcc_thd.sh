cd ..
rm outputs/stats.json

wh=1
for i in 0 1 2 #3 4
do
for thd in 120 96 64 32 16 8 4 2 1 #1 2 4 8 16 32 64 96 #120
do
for alg in BAMBOO #WAIT_DIE SILO WOUND_WAIT BAMBOO NO_WAIT
do
		python test.py experiments/tpcc.json THREAD_CNT=${thd} NUM_WH=${wh} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done

#fname="tpcc-thd-wh1"
fname="tpcc-thd-wh1_bb_retire"
cd outputs/
python3 collect_stats.py
mv stats.csv tpcc/${fname}.csv
mv stats.json tpcc/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
