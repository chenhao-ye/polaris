cp -r config_test.h config.h
wl='ycsb'
python wait_die.py ${wl} &> time.out
