rm debug.out
cp -r config_tpcc_debug.h config.h
#cp -r config_real.h config.h
wl='tpcc'
#wl='ycsb'
python test_debug.py ${wl} "CLV" |& tee debug.out
#python test_debug.py ${wl} "WOUND_WAIT" |& tee debug.out
#python test_debug.py ${wl} "WAIT_DIE" |& tee debug.out
