rm outputs/*.out
cp -r config_real.h config.h
for i in 1 2 3 
do
	for wl in  'tpcc'
	do
		echo $wl
		timeout 300 python test_debug.py ${wl} "WAIT_DIE,NO_WAIT,WOUND_WAIT" &> outputs/${wl}_$i.out
		cd outputs
		python collect_stats.py ${wl}_$i.out
		cd ..
	done
done
