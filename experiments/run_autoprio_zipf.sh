exper=autoprio_zipf
mkdir -p results
thd=64

for zipf in 0.9 0.99 1.1 1.2 1.3 1.4 1.5; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
		mkdir -p "${data_dir}"
		python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} | tee "${data_dir}/log"
	done
done

python3 parse.py "${exper}"
