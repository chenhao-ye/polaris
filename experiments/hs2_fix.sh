cd ..
rm outputs/stats.json

for i in 0 1 2 #3 4
do
for l in 0 #0.15 0
do
for fix in 0 1
do
for pos in 0 0.25 0.5 0.75 1
do
for alg in BAMBOO #WOUND_WAIT BAMBOO 
do
for thd in 32
do
		python test.py experiments/synthetic_ycsb.json BB_LAST_RETIRE=$l FIXED_HS=${fix} NUM_HS=2 THREAD_CNT=${thd} SPECIFIED_RATIO=${pos} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done
done
done
done

fname="hs2_fix_bb_no_retire"
cd outputs/
python3 collect_stats.py
mv stats.csv synthetic/${fname}.csv
mv stats.json synthetic/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
