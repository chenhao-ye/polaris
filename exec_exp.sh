cp -r config_real.h config.h

wl="TPCC"
epochs=1
threads=20
cnt=100000
wh=1

for alg in "NO_WAIT" "WAIT_DIE" "WOUND_WAIT" "CLV"
do
	for penalty in 1 10 1000 10000 100000
	do
		for i in $(seq 1 $epochs) 
		do
			timeout 300 python test_debug.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a  outputs/log.out
		done
	done
done

cd outputs
python collect_stats.py log.out
cd ..

