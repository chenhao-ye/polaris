cd ..
rm outputs/stats.json

pos=0
for i in 0 1 2 3 4
do
for thd in 1 2 4 8 16 32 64 120
do
for req in 4 16 64 
do
for alg in  WOUND_WAIT BAMBOO 
do
		python test.py experiments/synthetic_ycsb.json THREAD_CNT=${thd} REQ_PER_QUERY=${req} YSPECIFIED_RATIO=${pos} CC_ALG=${alg} OUTPUT_TO_FILE=true CPU_FREQ=2.8
done
done
done

fname="hs1_req"
cd outputs/
python3 collect_stats.py
mv stats.csv synthetic/${fname}.csv
mv stats.json synthetic/${fname}.json
cd ..

cd experiments
python3 send_email.py ${fname}
