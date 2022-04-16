exper=tpcc_latency
mkdir -p results
num_wh=1
thd=64

for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
	data_dir="results/${exper}/TPCC-CC=${alg}-THD=${thd}-NUM_WH=${num_wh}"
	mkdir -p "${data_dir}"
	python3 test.py experiments/tpcc.json THREAD_CNT=${thd} CC_ALG=${alg} NUM_WH=${num_wh} DUMP_LATENCY=true DUMP_LATENCY_FILENAME="\"${data_dir}/latency_dump.csv\"" | tee "${data_dir}/log"
done

python3 parse.py "${exper}"

