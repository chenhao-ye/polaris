rm debug.out
cp -r config_tpcc_debug.h config.h

wl="TPCC"
threads=10
cnt=100000
wh=1
penalty=10

for alg in "CLV" "NO_WAIT" "WAIT_DIE" "WOUND_WAIT" 
#for alg in "CLV"
do
	timeout 300 python test_debug.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a debug.out
done
