exper=autoprio_zipf
mkdir -p results

thd=16
for zipf in 0.1 0.3 0.5 0.7 0.9 0.99 1.1 1.3 1.5; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		python2 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
	done
done

# just to check if thd=32 has consistent behavior as thd=16
thd=32
for zipf in 0.1 0.3 0.5 0.7 0.9 0.99 1.1 1.3 1.5; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		python2 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
	done
done

python3 parse.py "${exper}"
