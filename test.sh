rm debug.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=10
cnt=100000
wh=3
penalty=1

#for alg in "CLV" "NO_WAIT" "WAIT_DIE" "WOUND_WAIT" 
for i in 1 2 3
do
for wh in 1 2 3 4
do
for alg in "HEKATON"
do
	timeout 300 python test_debug.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a debug.out
done
done
done


for i in 1 2 3
do
for threads in 5 10 20
do
for alg in "HEKATON"
do
	timeout 300 python test_debug.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a debug.out
done
done
done
