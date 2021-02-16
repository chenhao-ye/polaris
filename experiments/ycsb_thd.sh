cd ..
rm outputs/stats.json

zipf=0.9
for l in 0.15
do
for i in 0 1 2 #3 4
do
for thd in 120 96 64 32 16 8 4 2 1 #1 2 4 8 16 32 64 96 #120
do
for alg in BAMBOO #WAIT_DIE SILO WOUND_WAIT BAMBOO NO_WAIT
do
		python test.py experiments/large_dataset.json BB_LAST_RETIRE=$l THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done
done

fname="ycsb-thd-100g_bb_retire"
cd outputs/
python3 collect_stats.py
mv stats.csv ycsb_thd/${fname}.csv
mv stats.json ycsb_thd/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
