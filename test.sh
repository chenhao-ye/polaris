rm debug-1.out
cp -r config_tpcc_debug.h config.h
#cp -r config_real.h config.h
wl='tpcc'
#wl='ycsb'
#python test_debug.py ${wl} "CLV,NO_WAIT,WOUND_WAIT,WAIT_DIE" |& tee debug-1.out
python test_debug.py ${wl} "CLV" |& tee -a debug-1.out
python test_debug.py ${wl} "NO_WAIT" |& tee -a debug-1.out
python test_debug.py ${wl} "WOUND_WAIT" |& tee -a debug-1.out
python test_debug.py ${wl} "WAIT_DIE" |& tee -a  debug-1.out
