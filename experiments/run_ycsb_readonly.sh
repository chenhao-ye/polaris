exper=ycsb_readonly
mkdir -p results
zipf=0.99

for thd in 1 4 8 16 24 32 40 48 56 64; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		if [ "$thd" != "64" ] && [ "$thd" != "16" ]; then
			python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} READ_PERC=1 | tee "${data_dir}/log"
		else
			python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} READ_PERC=1 DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
		fi
	done
done

python3 parse.py "${exper}"
