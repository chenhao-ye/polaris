cd ..
rm outputs/stats.json

thd=32
for i in 0 1 2 #3 4
do
for wh in 1 2 4 8 16 32 64 128
do
for alg in BAMBOO  #WAIT_DIE SILO WOUND_WAIT BAMBOO NO_WAIT
do
		python test.py experiments/tpcc.json THREAD_CNT=${thd} NUM_WH=${wh} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done

#fname="tpcc-wh-thd32"
fname="tpcc-wh-thd32_bb_retire"
cd outputs/
python3 collect_stats.py
mv stats.csv tpcc/${fname}.csv
mv stats.json tpcc/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
