exper=ycsb_zipf
mkdir -p results
thd=64

for zipf in 0 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 0.99 1.1 1.2 1.3 1.4 1.5; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		if [ "$zipf" != "0" ] && [ "$zipf" != "0.9" ] && [ "$zipf" != "0.99" ] && [ "$zipf" != "1.5" ]; then
			python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} | tee "${data_dir}/log"
		else
			python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
		fi
	done
done

python3 parse.py "${exper}"
