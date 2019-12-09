rm outputs/log.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
epochs=3
threads=20
cnt=100000
wh=1

for cnt in 10000 100000 1000000
do
for alg in "NO_WAIT" "WAIT_DIE" "WOUND_WAIT" "CLV"
#for alg in "CLV"
do
	for penalty in 1 10 100
	do
		for threads in 5 10 20
		do
			for wh in 1 2 3 4
			do
				for i in $(seq 1 $epochs) 
				do
					timeout 300 python test_debug.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a  outputs/log.out
				done
			done
		done
	done
done
done

cd outputs
python collect_stats.py log.out
cd ..

