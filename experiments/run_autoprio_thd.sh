exper=autoprio_thd
mkdir -p results
zipf=0.9

for thd in 1 2 4 8 16 32 64; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/YCSB_CC=${alg}_THD=${thd}_ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		python2 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
	done
done
