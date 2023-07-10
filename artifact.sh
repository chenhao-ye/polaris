#!/bin/bash
# assume the current working directory is `polaris/`
sudo apt update
sudo apt install -y numactl python3-pip
pip3 install -r requirements.txt

bash experiments/run_all.sh
python3 parse.py ycsb_latency ycsb_prio_sen ycsb_thread ycsb_readonly ycsb_zipf tpcc_thread ycsb_aria_batch
python3 plot.py
