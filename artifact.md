
# Artifact Reproduction

This document aims to provide detailed step-by-step instructions to reproduce all experiments on a CloudLab c6420 instance with Ubuntu 20. We recommend using this environment to reproduce experiments. To run experiments on other hardware, make sure the machine has at least 64 logical cores since some experiments use 64 threads.

To begin with, you may download a copy of the [paper](https://dl.acm.org/doi/10.1145/3588724?cid=99660889005).

## Step 0-A: Set up c6420 Machine

CloudLab machines, by default, only have 16 GB of storage space mounted at the root, which can be insufficient. The script below mounts `/dev/sda4` to `~/workspace`. You may skip this step if not using CloudLab c6420 instances.

```bash
DISK=/dev/sda4
WORKSPACE=$HOME/workspace
sudo mkfs.ext4 $DISK
sudo mkdir $WORKSPACE
sudo mount $DISK $WORKSPACE
sudo chown -R $USER $WORKSPACE
echo "$DISK        $WORKSPACE        ext4    defaults        0       0" | sudo tee -a /etc/fstab
# this directory will be mounted automatically for every reboot
```

## Step 0-B: Install Dependencies

Download the codebase under `~/workspace`, and `cd` into Polaris top-level directory. Then install software dependencies:

```bash
# assume the current working directory is `polaris/`
sudo apt update
sudo apt install -y numactl python3-pip
pip3 install -r requirements.txt
```

## Step 1: Run All Experiments

The experiment scripts are under `experiments/`. To run all experiments:

```bash
# run all experiments; this may take hours
bash experiments/run_ycsb_latency.sh  # fig 1, 7
bash experiments/run_ycsb_prio_sen.sh # fig 2
bash experiments/run_ycsb_thread.sh   # fig 3
bash experiments/run_ycsb_readonly.sh # fig 4
bash experiments/run_ycsb_zipf.sh     # fig 5, 6
bash experiments/run_tpcc_thread.sh   # fig 8, 9
bash experiments/run_ycsb_aria.sh     # fig 10, 11
```

Alternatively, simply run `bash experiments/run_all.sh`, which is a shortcut to run the lines above.

The experiment results are saved under `results/`. You should find 7 subdirectories: `ycsb_latency`, `ycsb_prio_sen`, `ycsb_thread`, `ycsb_readonly`, `ycsb_zipf`, `tpcc_thread`, and `ycsb_aria_batch`.

## Step 2: Process Experiment Data

Then parse and plot the experiment results:

```bash
# parse all experiments
python3 parse.py ycsb_latency ycsb_prio_sen ycsb_thread ycsb_readonly ycsb_zipf tpcc_thread ycsb_aria_batch

# plot figures; this may take minutes
python3 plot.py
```

The plots are saved in the current working directory. You should find 11 figures (with the corresponding figure numbers in the [paper](https://doi.org/10.1145/3588724)):

- fig 1: `ycsb_latency_allcc.pdf`
- fig 2: `ycsb_prio_ratio_vs_throughput.pdf`
- fig 3: `ycsb_thread_vs_throughput_tail.pdf`
- fig 4: `ycsb_thread_vs_throughput_tail_readonly.pdf`
- fig 5: `ycsb_zipf_vs_throughput_tail.pdf`
- fig 6: `ycsb_latency_udprio.pdf`
- fig 7: `tpcc_thread_vs_throughput_tail_wh1.pdf`
- fig 8: `tpcc_thread_vs_throughput_tail_wh64.pdf`
- fig 9: `ycsb_aria_thread_vs_throughput_tail_zipf0.99.pdf`
- fig 10: `ycsb_aria_thread_vs_throughput_tail_zipf0.5.pdf`

If running on CloudLab c6420 instance, all figures should be similar; the exception is fig 10: as reported in the paper, Aria p999 tail latency tends to have high variation due to batching.
