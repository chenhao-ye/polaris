rm debug.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=10
cnt=100000
wh=3
penalty=1

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
