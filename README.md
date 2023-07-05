# DBx1000-Polaris

[Polaris](https://dl.acm.org/doi/10.1145/3588724?cid=99660889005) is an optimistic concurrency control algorithm with priority support. This repository implements Polaris on top of [DBx1000](https://github.com/yxymit/DBx1000) and [DBx1000-Bamboo](https://github.com/ScarletGuo/Bamboo-Public).

- DBx1000: [Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores](http://www.vldb.org/pvldb/vol8/p209-yu.pdf). Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker.
- Bamboo: [Releasing Locks As Early As You Can: Reducing Contention of Hotspots by Violating Two-Phase Locking](https://doi.org/10.1145/3448016.3457294). Zhihan Guo, Kan Wu, Cong Yan, Xiangyao Yu.

These repositories implement other concurrency control algorithms (e.g., No-Wait, Wait-Die, Wound-Wait, Silo) as the baseline for Polaris evaluation.

This README describes the general usage of this repository; to reproduce all experiments in the [paper](https://dl.acm.org/doi/10.1145/3588724?cid=99660889005), please refer to [`ARTIFACT.md`](ARTIFACT.md).

## Quick Start: Build & Test

To test the database

```shell
python3 test.py experiments/default.json
```

The command above will compile the code with the configuration specified in `experiments/default.json` and run experiments. `test.py` will read the configuration and the existing `config-std.h` to generate a new `config.h`.

You can find other configuration files (`*.json`) under `experiments/`.

## Advanced: Configure & Run

The parameters are set by `config-std.h` and the configuration file. You could overwrite parameters by specifying them from the command-line.

```shell
python3 test.py experiments/default.json COMPILE_ONLY=true
```

This command would only compile the code but not run the experiment.

Below are parameters that affect `test.py` behavior:

- `UNSET_NUMA`: If set false, it will interleavingly allocate data. Default is `false`.
- `COMPILE_ONLY`: Only compile the code but not run the experiments. Default is `false`.
- `NDEBUG`: Disable all `assert`. Default is `true`.

Below is a list of basic build parameters. They typically turn certain features on or off for evaluation purposes. The list is not exhaustive and you can find more on `config-std.h`.

- `CC_ALG`: Which concurrency control algorithm to use. Default is `SILO_PRIO`, which is an alias name of Polaris\*.
- `THREAD_CNT`: How many threads to use.
- `WORKLOAD`: Which workload to run. Either `YCSB` or `TPCC`.
- `ZIPF_THETA`: What is the Zipfian theta value in YCSB workload. Only useful when `WORKLOAD=YCSB`.
- `NUM_WH`: How many warehouses in TPC-C workload. Only useful when `WORKLOAD=TPCC`.
- `DUMP_LATENCY`: Whether dump the latency of all transactions to a file. Useful for latency distribution plotting.
- `DUMP_LATENCY_FILENAME`: If `DUMP_LATENCY=true`, what's the filename of the dump.


Below is another list of build parameters introduced for Polaris:

- `SILO_PRIO_NO_RESERVE_LOWEST_PRIO`: Whether turn on the lowest-priority optimization for Polaris. Default is `true` and it should be set true all the time (unless you want to benchmark how much gain from this optimization).
- `SILO_PRIO_FIXED_PRIO`: Whether fix the priority of each transaction. If `false`, Polaris will assign priority based on its own policy.
- `SILO_PRIO_ABORT_THRESHOLD_BEFORE_INC_PRIO`: Do not increment the priority until the transaction's abort counter reaches this threshold.
- `SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT`: After reaching the threshold, increment the priority by one for every specified number of aborts.
- `HIGH_PRIO_RATIO`: What's the ratio of transactions that start with high (i.e., nonzero) priority. Useful to simulate the case of user-specified priority.

There are other handy tools included in this repository. `experiments/*.sh` are scripts to reproduce the experiments described in our paper. `parse.py` will process the experiment results into CSV files and `plot.py` can visualize them.

\* **Fun fact**: Polaris is implemented based on Silo but with priority support, so it was previously termed `SILO_PRIO`. The name `POLARIS` came from a letter rearrangement of `SILO_PRIO` with an additional `A`.
