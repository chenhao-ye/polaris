exper=fixedprio_binary_thd
mkdir -p results
zipf=0.9

for thd in 1 2 4 8 16 32 64; do
	for alg in SILO SILO_PRIO; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		python2 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} HIGH_PRIO_RATIO=0.1 SILO_PRIO_FIXED_PRIO=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
	done
done

python3 parse.py "${exper}"
