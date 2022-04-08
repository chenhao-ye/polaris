exper=tpcc_wh
mkdir -p results
thd=64

for num_wh in 1 2 4 8 16 32 64; do
	for alg in SILO SILO_PRIO WOUND_WAIT NO_WAIT WAIT_DIE; do
		data_dir="results/${exper}/TPCC-CC=${alg}-THD=${thd}-NUM_WH=${num_wh}"
		mkdir -p "${data_dir}"
		python3 test.py experiments/tpcc.json THREAD_CNT=${thd} CC_ALG=${alg} NUM_WH=${num_wh} | tee "${data_dir}/log"
	done
done

python3 parse.py "${exper}"
