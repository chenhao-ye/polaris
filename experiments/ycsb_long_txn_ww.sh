cd ..
cp outputs/ycsb_long_txn.json outputs/stats.json

zipf=0.9
for i in 0 1 2 3 4
do
for thd in 64 #1 2 4 8 16 32 64
do
for alg in WOUND_WAIT 
do
		python test.py experiments/long_txn.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8 MAX_RUNTIME=10
done
done
done

fname="ycsb_long_txn_rerun"
cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
