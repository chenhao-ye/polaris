exper=ycsb_prio_sen
mkdir -p results
zipf=0.99
thd=64
alg=SILO_PRIO

for pr in 0 0.05 0.1 0.2 0.3 0.4 0.5 0.6 0.7 0.8 0.9 1; do
	data_dir="results/${exper}/YCSB-CC=${alg}-THD=${thd}-ZIPF=${zipf}-PRIO_RATIO=${pr}"
	mkdir -p "${data_dir}"
	python3 test.py experiments/large_dataset.json THREAD_CNT=${thd} ZIPF_THETA=${zipf} CC_ALG=${alg} SILO_PRIO_FIXED_PRIO=true HIGH_PRIO_RATIO=${pr} | tee "${data_dir}/log"
done

python3 parse.py "${exper}"
