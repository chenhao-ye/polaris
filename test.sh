rm -f out
cp -r config_test.h config.h
#python ww.py ycsb > out
python ww.py tpcc > out
