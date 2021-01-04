cd ../
cp -r config-std.h config.h

# General Config
threads=16
cnt=1000000
penalty=50000
latch=LH_MCSLOCK
unset_numa="false"
terminate_cnt="true"
runtime=10
## profiling options
pf="true"
pf_cs="false"
pf_abort="false"

# Algorithm-specific Config
## [WW]
ww_starv_free="false"
## [BAMBOO]
dynamic="true"
opt_raw="true"
last_retire=0
precommit="false"
autoretire="true"

# Workload Config
wl="YCSB"
## YCSB config
synthetic=false
### general config
zipf=0.9
read_ratio=0.5
long_ratio=0
long_req=1000
table_size="100000000"
req=16
### synthetic config
num_hs=0
pos=TOP
specified=0
fixed=1
fhs="WR"
shs="WR"
ordered="false"
flip=0
## TPCC config
wh=1

# Debugging Config


python test.py THREAD_CNT=$threads MAX_TXN_PER_PART=$cnt ABORT_PENALTY=$penalty LATCH=${latch} UNSET_NUMA=${unset_numa} TERMINIATE_BY_COUNT=${terminate_cnt} MAX_RUNTIME=${runtime}  
