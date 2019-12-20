rm debug.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=10
cnt=100000
wh=3
penalty=1

for alg in "CLV" "WOUND_WAIT"
do
	timeout 300 python test.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a debug.out
done
