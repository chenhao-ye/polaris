cd ..
cp outputs/ycsb_other/ycsb-read-ratio-100g.json outputs/stats.json

thd=32
zipf=0.9
for i in 0 1 2 3 4
do
for ratio in 0.1 0.3 0.7 0.9 #0.5
do
for alg in BAMBOO SILO WOUND_WAIT WAIT_DIE NO_WAIT
do
		python test.py experiments/large_dataset.json READ_PERC=${ratio} THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done

fname="ycsb-read-ratio-100g"
cd outputs/
python3 collect_stats.py
mv stats.csv ycsb_other/${fname}.csv
mv stats.json ycsb_other/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
