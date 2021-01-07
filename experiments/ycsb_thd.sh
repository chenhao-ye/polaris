cd ..

for zipf in 0 0.6 0.99
do
for thd in 1 2 4 8 16 32
do
for alg in BAMBOO SILO WOUND_WAIT WAIT_DIE NO_WAIT
do
		python test.py experiments/default.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} COMPILE_ONLY=true
		for i in 0 1 2 3 4
		do
			python test.py experiments/default.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} EXEC_ONLY=true OUTPUT_TO_FILE=true 
		done
done
done
done

fname="ycsb_thd"
cd outputs/
python3 collect_stats.py
mv stats.csv ${fname}.csv
mv stats.json ${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
