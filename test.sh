rm debug.out
#cp -r config_tpcc_debug.h config.h
cp -r config_ycsb_debug.h config.h

wl="TPCC"
wl="YCSB"
threads=20
cnt=100000
wh=1
penalty=1

for alg in "CLV" "WOUND_WAIT" "WAIT_DIE" "NO_WAIT" 
do
	timeout 100 python test.py ${wl} $alg $threads $cnt $penalty $wh |& tee -a debug.out
done
