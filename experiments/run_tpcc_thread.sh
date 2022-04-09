exper=tpcc_thread
mkdir -p results
num_wh=1

for thd in 1 4 8 16 24 32 40 48 56 64; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/TPCC-CC=${alg}-THD=${thd}-NUM_WH=${num_wh}"
		mkdir -p "${data_dir}"
		python3 test.py experiments/tpcc.json THREAD_CNT=${thd} CC_ALG=${alg} NUM_WH=${num_wh} | tee "${data_dir}/log"
	done
done

num_wh=64

for thd in 1 4 8 16 24 32 40 48 56 64; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/TPCC-CC=${alg}-THD=${thd}-NUM_WH=${num_wh}"
		mkdir -p "${data_dir}"
		python3 test.py experiments/tpcc.json THREAD_CNT=${thd} CC_ALG=${alg} NUM_WH=${num_wh} | tee "${data_dir}/log"
	done
done

python3 parse.py "${exper}"
