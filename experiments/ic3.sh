cd ..
rm outputs/stats.json

mode=true
for i in 0 1 2 #3 4
do
for l in 0.15
do
for thd in 1 2 4 8 16 32 64 96 120
do
for alg in BAMBOO #SILO WOUND_WAIT BAMBOO IC3 
do
		python test.py experiments/ic3.json BB_LAST_RETIRE=$l IC3_MODIFIED_TPCC=${mode} THREAD_CNT=${thd} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done
done

mode=false
#for i in 0 1 2 3 4
#do
#for thd in 1 2 4 8 16 32 64 96 120
#do
#for alg in IC3 
#do
#		python test.py experiments/ic3.json IC3_MODIFIED_TPCC=${mode} THREAD_CNT=${thd} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
#done
#done
#done

fname="ic3_bb_retire"
cd outputs/
python3 collect_stats.py
mv stats.csv tpcc/${fname}.csv
mv stats.json tpcc/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
