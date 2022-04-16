exper=ycsb_latency
mkdir -p results
zipf=0.99
thd=64

for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
	data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
	mkdir -p "${data_dir}"
	python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
done

python3 parse.py "${exper}"
