exper=ycsb_latency
mkdir -p results
zipf=0.99
thd=64

for alg in SILO WOUND_WAIT NO_WAIT WAIT_DIE; do
	data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
	mkdir -p "${data_dir}"
	python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
done

alg=SILO_PRIO
data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
mkdir -p "${data_dir}"
python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} HIGH_PRIO_RATIO=0.05 DUMP_LATENCY=true SILO_PRIO_FIXED_PRIO=false LOW_PRIO_BOUND=7 DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"

alg=SILO_PRIO_FIXED
data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}"
mkdir -p "${data_dir}"
python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=SILO_PRIO HIGH_PRIO_RATIO=0.05 DUMP_LATENCY=true SILO_PRIO_FIXED_PRIO=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"

python3 parse.py "${exper}"
