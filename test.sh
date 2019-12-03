rm debug.out
cp -r config_clv_debug.h config.h
#cp -r config_real.h config.h
wl='tpcc'
#wl='ycsb'
#python ww.py ${wl} &> debug.out
python test_debug.py ${wl} "WOUND_WAIT,CLV" |& tee debug.out
