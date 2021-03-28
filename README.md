DBx1000-Bamboo
==============
The repository is built on DBx1000: https://github.com/yxymit/DBx1000 

    Staring into the Abyss: An Evaluation of Concurrency Control with One Thousand Cores
    Xiangyao Yu, George Bezerra, Andrew Pavlo, Srinivas Devadas, Michael Stonebraker
    http://www.vldb.org/pvldb/vol8/p209-yu.pdf
    
The major changes made in this repository:
- added support for Bamboo and its optimizations. Bamboo is a concurrency control protocol proposed in:
```
    Releasing Locks As Early As You Can: Reducing Contention of Hotspots by Violating Two-Phase Locking
    Zhihan Guo, Kan Wu, Cong Yan, Xiangyao Yu
    link (TBA)
```
- focused on support for: NO_WAIT, WOUND_WAIT, WAIT_DIE, SILO, IC3
- changed the memory allocation for lock entry to be static
- added support for MCS Lock in addition to mutex and spinlock
- modified test scripts for easier evaluation


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








