rm outputs/*.out
for i in 1 2 3 4 5
do
	for wl in 'ycsb' 'tpcc'
	do
		echo $wl
		python wait_die.py ${wl} &> outputs/${wl}_$i.out
		cd outputs
		python collect_stats.py ${wl}_$i.out
		cd ..
	done
done
