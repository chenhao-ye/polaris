#!/bin/bash
# assume the current working directory is `polaris/`
bash experiments/run_ycsb_latency.sh  # fig 1, 7
bash experiments/run_ycsb_prio_sen.sh # fig 2
bash experiments/run_ycsb_thread.sh   # fig 3
bash experiments/run_ycsb_readonly.sh # fig 4
bash experiments/run_ycsb_zipf.sh     # fig 5, 6
bash experiments/run_tpcc_thread.sh   # fig 8, 9
bash experiments/run_ycsb_aria.sh     # fig 10, 11
