cd ..

for i in 0 1 2 3 4
for zipf in 0.9
do
for thd in 1 2 4 8 16 32 64 96 128
do
for alg in BAMBOO SILO WOUND_WAIT WAIT_DIE NO_WAIT
do
		python test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} OUTPUT_TO_FILE=true
done
done
done

fname="ycsb_thd_large"
cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
