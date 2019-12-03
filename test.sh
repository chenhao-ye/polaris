rm debug.out
cp -r config_debug.h config.h
#cp -r config_real.h config.h
wl='tpcc'
wl='ycsb'
#python ww.py ${wl} &> debug.out
python ww.py ${wl} |& tee debug.out
