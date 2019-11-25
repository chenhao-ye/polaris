cp -r config_debug.h config.h
wl='ycsb'
#python ww.py ${wl} &> debug.out
python ww.py ${wl} |& tee debug.out
