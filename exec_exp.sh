rm outputs/*.out
cp -r config_real.h config.h

wl="tpcc"
epochs=3
threads=20
cnt=10000

for i in $(seq 1 $epochs) 
do
	for penalty in 1 10 1000 10000 100000
	do
		echo $wl
		timeout 300 python test_debug.py ${wl} "WAIT_DIE,NO_WAIT,WOUND_WAIT,CLV" $threads $cnt $penalty &> outputs/${penalty}_$i.out
		cd outputs
		python collect_stats.py ${penalty}_$i.out
		cd ..
	done
done
