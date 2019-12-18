rm outputs/log.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
epochs=3
threads=20
cnt=100000
wh=1

for i in $(seq 1 $epochs) 
do
for wh in 16 8 4 2 1
#for alg in "CLV"
do
	for penalty in 1
	do
		for threads in 32 16 8 4 2 1
		do
			for alg in "CLV" "NO_WAIT" "WAIT_DIE" "WOUND_WAIT" 
			do
				for cnt in 100000 
				do
					timeout 300 python test.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a  outputs/log.out
				done
			done
		done
	done
done
done

cd outputs
python collect_stats.py log.out
cd ..

