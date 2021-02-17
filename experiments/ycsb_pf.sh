cd ..
rm outputs/stats.json

thd=120
for l in 0.15
do
for i in 0  #3 4
do
for zipf in 0.9 #0.5 0.7 0.8 0.9 0.99 
do
for alg in BAMBOO #BAMBOO SILO WOUND_WAIT WAIT_DIE NO_WAIT
do
		python test.py experiments/large_dataset.json PF_CS=true BB_LAST_RETIRE=$l THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done
done

fname="ycsb-pf"
cd outputs/
python3 collect_stats.py
mv stats.csv ycsb_other/${fname}.csv
mv stats.json ycsb_other/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
