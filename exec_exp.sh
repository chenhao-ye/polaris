rm outputs/*.out
cp -r config_real.h config.h
for i in 1 2 3 
do
	for wl in 'ycsb' 'tpcc'
	do
		echo $wl
		timeout 300 python wait_die.py ${wl} &> outputs/${wl}_$i.out
		cd outputs
		python collect_stats.py ${wl}_$i.out
		cd ..
	done
done
