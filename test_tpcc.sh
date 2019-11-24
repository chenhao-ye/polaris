cp -r config_real_debug.h config.h
wl='tpcc'
python ww.py ${wl} &> tpcc.out
