cd ..
#rm outputs/stats.json
cp outputs/ycsb_long_txn_ww.json outputs/stats.json

zipf=0.9
for i in 0 1 2 3 4
do
for thd in 96 120 #1 2 4 8 16 32 64
do
for alg in BAMBOO SILO WOUND_WAIT WAIT_DIE NO_WAIT
do
		python test.py experiments/long_txn.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8 MAX_RUNTIME=10 
done
done
done

fname="ycsb_long_txn_large_core"
cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
