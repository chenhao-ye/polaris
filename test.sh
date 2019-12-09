rm debug.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=3
cnt=10000
wh=1
penalty=10

#for alg in "NO_WAIT" "WAIT_DIE" "WOUND_WAIT" "CLV"
for alg in "CLV"
do
	timeout 300 python test_debug.py ${wl} $alg $threads $cnt $penalty $wh |& tee  debug.out
done
