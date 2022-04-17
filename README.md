<meta name="robots" content="noindex">

DBx1000-Bamboo
==============
The repository is built on DBx1000: https://github.com/yxymit/DBx1000 and https://github.com/ScarletGuo/Bamboo-Public

    Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
    Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
    http://www.vldb.org/pvldb/vol8/p209-yu.pdf
    

    Releasing Locks As Early As You Can: Reducing Contention of Hotspots by Violating Two-Phase Locking
    Zhihan Guo, Kan Wu, Cong Yan, Xiangyao Yu
    https://doi.org/10.1145/3448016.3457294

Build & Test
------------

To test the database

    python test.py experiments/default.json

    
Configure & Run
---------------

Supported configuration parameters can be found in config-std.h file. Extra configuration parameters include: 
```
    UNSET_NUMA        : default is false. If set false, it will disable numa effect by interleavingly allocate data. 
    NDEBUG            : default is true. If set true, it will disable all assert()
    COMPILE_ONLY      : defalut is false. If set false, it will compile first and then automatically execute. 
```
Options to change/pass configuration:
- Option 1: use basic configurations provided in experiments/default.json. overwrite existing configurations or pass extra configurations through arguments. 
    e.g. ```python test.py experiments/default.json WORKLOAD=TPCC NUM_WH=1```
- Option 2: directly copy config-std.h to config.h and modify config.h. Then compile using ```make -j``` and execute through ```./rundb ```

`experiments` directory contains some handy scripts for experiments with varying configurations. `parse.py` will process the experiments results into CSV files and `plot.py` can visualize them.

Experiment Configuration
---------------

These configuration are newly added to support priority-related experiments. Note they are not exhaustive, and more details can be found by reading `config-std.h`.
```
    SILO_PRIO_FIXED_PRIO               : whether use fixed priority.
        Default is false. If set true, a transaction will has not change its
        priority after abort.
    SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT : increment priority after every certain
        number of aborts.
        Default is 3. It controls how frequency a transaction will be prompted.
        This option is only effective if SILO_PRIO_FIXED_PRIO is false.
    SILO_PRIO_ABORT_THRESHOLD_BEFORE_INC_PRIO : threshold before SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT starts to work.
        Default is 8.
    HIGH_PRIO_RATIO                    : how many ratio of transactions begin
        with priority 8 (others begin with priority zero).
        Default is 0. It must be a number between 0 and 1. This flag is useful
        with SILO_PRIO_FIXED_PRIO=true for a binary priority case.
    DUMP_LATENCY                       : whether dump all latency into a file.
        Default is true. Useful for plotting.
    DUMP_LATENCY_FILENAME              : the name of latency file to dump.
        Default is "latency_dump.csv".
```

The options above could be used as some combos.

- Example 1: all transactions begin with priority zero and increment priority every 4 aborts.

    ```
    SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT=4
    ```

- Example 2: only two levels of priority: high/low; 5% of transactions are high-priority and 95% are low

    ```
    SILO_PRIO_FIXED_PRIO=true HIGH_PRIO_RATIO=0.05
    ```

- Example 3: same setting as example 1 but try to dump the latency file into "latency_inc4.csv"

    ```
    SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT=4 DUMP_LATENCY_FILENAME=\"latency_inc4.csv\"
    ```

    Note here it's assumed to be fed as the command-line argument and subject to shell grammar, which is necessary to escape the quotation marks.
