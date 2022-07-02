exper=ycsb_aria_batch
mkdir -p results

alg=ARIA
for zipf in 0.99 0.5; do
	for thd in 1 4 8 16 24 32 40 48 56 64; do
		for batch in 1 2 4 8; do
			data_dir="results/${exper}/YCSB-CC=${alg}_${batch}-THD=${thd}-ZIPF=${zipf}"
			mkdir -p "${data_dir}"
			if [ "$thd" != "64" ] && [ "$thd" != "16" ]; then
				python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} ARIA_BATCH_SIZE=${batch} | tee "${data_dir}/log"
			else
				python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} ARIA_BATCH_SIZE=${batch} DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
			fi
		done
	done
done

# we then run SILO_PRIO for comparsion
alg=SILO_PRIO
for zipf in 0.99 0.5; do
	for thd in 1 4 8 16 24 32 40 48 56 64; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		if [ "$thd" != "64" ] && [ "$thd" != "16" ]; then
			python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} | tee "${data_dir}/log"
		else
			python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
		fi
	done
done

python3 parse.py "${exper}"
