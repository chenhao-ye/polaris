#ifndef _CONFIG_H_
#define _CONFIG_H_

/***********************************************/
// Simulation + Hardware
/***********************************************/
#define TERMINATE_BY_COUNT         true
#define THREAD_CNT					20
#define PART_CNT					1
// each transaction only accesses 1 virtual partition. But the lock/ts manager and index are not aware of such partitioning. VIRTUAL_PART_CNT describes the request distribution and is only used to generate queries. For HSTORE, VIRTUAL_PART_CNT should be the same as PART_CNT.
#define VIRTUAL_PART_CNT			1
#define PAGE_SIZE					4096
#define CL_SIZE						64
// CPU_FREQ is used to get accurate timing info
#define CPU_FREQ 					2.6 // in GHz/s

// # of transactions to run for warmup
#define WARMUP						1000
// YCSB or TPCC
#define WORKLOAD                    YCSB
// print the transaction latency distribution
#define PRT_LAT_DISTR				false
#define STATS_ENABLE				true
#define TIME_ENABLE					true

#define MEM_ALLIGN					8

// [THREAD_ALLOC]
#define THREAD_ALLOC				false
#define THREAD_ARENA_SIZE			(1UL << 22)
#define MEM_PAD 					true

// [PART_ALLOC]
#define PART_ALLOC 					false
#define MEM_SIZE					(1UL << 30)
#define NO_FREE						false

/***********************************************/
// Concurrency Control
/***********************************************/
// WAIT_DIE, NO_WAIT, DL_DETECT, TIMESTAMP, MVCC, HEKATON, HSTORE, OCC, VLL, TICTOC, SILO
// TODO TIMESTAMP does not work at this moment
#define CC_ALG                      SILO_PRIO
#define ISOLATION_LEVEL 			SERIALIZABLE

// latch options
#define LATCH					    LH_SPINLOCK

// all transactions acquire tuples according to the primary key order.
#define KEY_ORDER					false
// transaction roll back changes after abort
#define ROLL_BACK					true
// per-row lock/ts management or central lock/ts management
#define CENTRAL_MAN					false
#define BUCKET_CNT					31
#define ABORT_PENALTY 				10000
#define ABORT_BUFFER_SIZE			1
#define ABORT_BUFFER_ENABLE			true
// [ INDEX ]
#define ENABLE_LATCH				false
#define CENTRAL_INDEX				false
#define CENTRAL_MANAGER 			false
#define INDEX_STRUCT				IDX_HASH
#define BTREE_ORDER 				16

// [DL_DETECT]
#define DL_LOOP_DETECT				1000 	// 100 us
#define DL_LOOP_TRIAL				100	// 1 us
#define NO_DL						KEY_ORDER
#define TIMEOUT						1000000 // 1ms
// [TIMESTAMP]
#define TS_TWR						false
#define TS_ALLOC					TS_CAS
#define TS_BATCH_ALLOC				false
#define TS_BATCH_NUM				1
// [MVCC]
// when read/write history is longer than HIS_RECYCLE_LEN
// the history should be recycled.
//#define HIS_RECYCLE_LEN		    10
//#define MAX_PRE_REQ				1024
//#define MAX_READ_REQ				1024
#define MIN_TS_INTVL				5000000 //5 ms. In nanoseconds
// [OCC]
#define MAX_WRITE_SET				10
#define PER_ROW_VALID				true
// [TICTOC]
#define WRITE_COPY_FORM				"data" // ptr or data
#define TICTOC_MV					false
#define WR_VALIDATION_SEPARATE		true
#define WRITE_PERMISSION_LOCK		false
#define ATOMIC_TIMESTAMP			"false"
// [TICTOC, SILO]
#define VALIDATION_LOCK				"no-wait" // no-wait or waiting
#define PRE_ABORT					"true"
#define ATOMIC_WORD					true
// [HSTORE]
// when set to true, hstore will not access the global timestamp.
// This is fine for single partition transactions.
#define HSTORE_LOCAL_TS				false
// [VLL]
#define TXN_QUEUE_SIZE_LIMIT		THREAD_CNT
// [BAMBOO]
#define BB_DYNAMIC_TS					true
#define BB_OPT_RAW                  true
#define BB_LAST_RETIRE                 0
#define BB_PRECOMMIT                false
#define BB_AUTORETIRE               false
#define BB_ALWAYS_RETIRE_READ       true
// [WW]
#define WW_STARV_FREE               true // set false if compared w/ bamboo
// [IC3]
#define IC3_EAGER_EXEC              true 
#define IC3_RENDEZVOUS              true
#define IC3_FIELD_LOCKING           false // should not be true
#define IC3_MODIFIED_TPCC           false
// [SILO_PRIO]
//   optimization: if true, the lowest-priority transaction does not place any
//   reserveration; this avoid a shared-memory write to TID
#define SILO_PRIO_NO_RESERVE_LOWEST_PRIO true
//   whether use fixed priority; if true, a query's priority won't be
//   incremented no matter how many times it has aborted
#define SILO_PRIO_FIXED_PRIO        false
//   don't increment the priority until specific number of aborts. for txns only
//   aborts 5 times don't contribute to the tail latency, so there is no point
//   to give these txns priority
#define SILO_PRIO_ABORT_THRESHOLD_BEFORE_INC_PRIO 8
//   after num_abort exceeds the threshold, start to increment priority
//   increment the priority of a query after how many aborts
//   only effective if SILO_PRIO_FIXED_PRIO is false
#define SILO_PRIO_INC_PRIO_AFTER_NUM_ABORT 3
//   how to distribute the number of bits in TID (change is not recommended...)
#define SILO_PRIO_NUM_BITS_PRIO_VER 4
#define SILO_PRIO_NUM_BITS_PRIO     4
#define SILO_PRIO_NUM_BITS_REF_CNT  10
#define SILO_PRIO_NUM_BITS_DATA_VER 45

static_assert(SILO_PRIO_NUM_BITS_PRIO_VER + SILO_PRIO_NUM_BITS_PRIO \
	+ SILO_PRIO_NUM_BITS_REF_CNT + SILO_PRIO_NUM_BITS_DATA_VER + 1 == 64,
	"TID must be exactly 64 bits");

// there is no SILO_PRIO_MAX_PRIO_VER, since we expect it to overflow
#define SILO_PRIO_MAX_PRIO     ((1 << (SILO_PRIO_NUM_BITS_PRIO)) - 1)
#define SILO_PRIO_MAX_REF_CNT  ((1 << (SILO_PRIO_NUM_BITS_REF_CNT)) - 1)
#define SILO_PRIO_MAX_DATA_VER ((1 << (SILO_PRIO_NUM_BITS_DATA_VER)) - 1)

#define SILO_PRIO_NUM_PRIO_LEVEL (1 << (SILO_PRIO_NUM_BITS_PRIO))

// [ARIA]
#define ARIA_NUM_BITS_BATCH_ID    32
#define ARIA_NUM_BITS_PRIO        4
#define ARIA_NUM_BITS_TXN_ID      28
static_assert(ARIA_NUM_BITS_BATCH_ID + ARIA_NUM_BITS_PRIO \
	+ ARIA_NUM_BITS_TXN_ID == 64);
#define ARIA_MAX_PRIO             ((1 << ARIA_NUM_BITS_PRIO) - 1)

#define ARIA_BATCH_SIZE           1
// if aborts and reexecute, whether uses a new txn_id or the previous one
#define ARIA_NEW_TXN_ID_REEXEC    false
// whether use pthread barrier or the one we implement; recommend NOT use
// pthread barrier, as we see significant performance drop
#define ARIA_USE_PTHREAD_BARRIER  false
// optimization: do not copy the row for read, because the whole database is
// read-only during execution phase
#define ARIA_NOCOPY_READ          true
// optimization: enable deterministic reordering
#define ARIA_REORDER              true

// Workload-related config:
//   priority distribution:
//     High priority txn will be given prio 1 as beginning, and low priority txn
//     will be given prio 0. Set this ratio (between 0 and 1) to set the ratio
//     of high-priority txn.
//     If the experiment is designed for auto-incremented priority at abort, it
//     is recommended to disable this feature (i.e. set to 0) so that every txn
//     starts with prio 0, though the codebase does support to have two flags
//     enabled together.
#define HIGH_PRIO_RATIO 0
//   priority growing space:
//     Low priority txn cannot grow its priority over the bound
#define LOW_PRIO_BOUND SILO_PRIO_MAX_PRIO
//   we collect the query distribution in terms of num_abort; for
//   abort_cnt < MAX_ABORT_CNT, we collect the exact number; for txn with more
//   abort, we simply keep a counter for all of them
#define STAT_MAX_NUM_ABORT 50
#define DUMP_LATENCY false
#define DUMP_LATENCY_FILENAME "latency_dump.csv"

/***********************************************/
// Logging
/***********************************************/
#define LOG_COMMAND					false
#define LOG_REDO					false
#define LOG_BATCH_TIME				10 // in ms

/***********************************************/
// Benchmark
/***********************************************/
#define INSERT_ENABLED              false
#define THINKTIME				    0
#define MAX_RUNTIME                 30 // in s, used only if !TERMINATE_BY_TIME
// max number of rows touched per transaction
#define MAX_ROW_PER_TXN				64
#define QUERY_INTVL 				1UL
#define MAX_TXN_PER_PART 			10000
#define FIRST_PART_LOCAL 			true
#define MAX_TUPLE_SIZE				1024 // in bytes
#define MAX_FIELD_SIZE                          50
// ==== [YCSB] ====
#define INIT_PARALLELISM			THREAD_CNT
#define SYNTH_TABLE_SIZE 			(1024 * 5)
#define ZIPF_THETA 					0.9
#define READ_PERC 					1
#define WRITE_PERC 					1  // if want no scan, write + read >= 1
#define SCAN_PERC 					0
#define SCAN_LEN					20
#define PART_PER_TXN 				1
#define PERC_MULTI_PART				1
#define REQ_PER_QUERY				16
#define LONG_TXN_RATIO			        0
#define LONG_TXN_READ_RATIO			0.5
#define FIELD_PER_TUPLE				10
// ==== [YCSB-synthetic] ====
#define SYNTHETIC_YCSB              true
#define POS_HS                      TOP
#define SPECIFIED_RATIO             0
#define FLIP_RATIO                  0
#define NUM_HS                      1
#define FIRST_HS                    WR
#define SECOND_HS                   WR
#define FIXED_HS                    0
// ==== [TPCC] ====
// For large warehouse count, the tables do not fit in memory
// small tpcc schemas shrink the table size.
#define TPCC_SMALL					false
// Some of the transactions read the data but never use them.
// If TPCC_ACCESS_ALL == fales, then these parts of the transactions
// are not modeled.
#define TPCC_ACCESS_ALL 			false
#define WH_UPDATE					true
#define NUM_WH 						1
//
enum TPCCTxnType {
  TPCC_PAYMENT,
  TPCC_NEW_ORDER,
  TPCC_DELIVERY,
  TPCC_ORDER_STATUS,
  TPCC_STOCK_LEVEL,
  TPCC_ALL};
extern TPCCTxnType 					g_tpcc_txn_type;

//#define TXN_TYPE					TPCC_ALL
#define PERC_PAYMENT 				0.5
#define PERC_DELIVERY               0
#define PERC_ORDERSTATUS            0
#define PERC_STOCKLEVEL             0
// PERC_NEWORDER is (1 - the sum of above).
#define FIRSTNAME_MINLEN 			8
#define FIRSTNAME_LEN 				16
#define LASTNAME_LEN 				16

#define DIST_PER_WARE				10
// enable user-initiated aborts in new-order txn according to TPC-C doc.
#define TPCC_USER_ABORT             true

// Optimizations used in IC3
#define COMMUTATIVE_OPS          false

/***********************************************/
// TODO centralized CC management.
/***********************************************/
#define MAX_LOCK_CNT				(20 * THREAD_CNT)
#define TSTAB_SIZE                  50 * THREAD_CNT
#define TSTAB_FREE                  TSTAB_SIZE
#define TSREQ_FREE                  4 * TSTAB_FREE
#define MVHIS_FREE                  4 * TSTAB_FREE
#define SPIN                        false

/***********************************************/
// Test cases
/***********************************************/
#define TEST_ALL					true
enum TestCases {
  READ_WRITE,
  CONFLICT
};
extern TestCases					g_test_case;

/***********************************************/
// DEBUG info
/***********************************************/
#define WL_VERB						true
#define IDX_VERB					false
#define VERB_ALLOC					true
#define DEBUG_LOCK					false
#define DEBUG_TIMESTAMP					false
#define DEBUG_SYNTH					false
#define DEBUG_ASSERT                			false
#define DEBUG_CC					false
#define DEBUG_WW                    			false
#define DEBUG_BENCHMARK             			false
#define DEBUG_BAMBOO                			false
#define DEBUG_TMP					false

/***********************************************/
// PROFILING 
/***********************************************/
#define PF_BASIC					false
#define PF_CS          					false // profiling inside critical path
#define PF_ABORT_LENGTH          			false
#define PF_MODEL          false

/***********************************************/
// Constant
/***********************************************/
// INDEX_STRUCT
#define IDX_HASH 					1
#define IDX_BTREE					2
// WORKLOAD
#define YCSB						1
#define TPCC						2
#define TEST						3
// latch options
#define LH_SPINLOCK                   1
#define LH_MUTEX                      2
#define LH_MCSLOCK                    3
// Concurrency Control Algorithm
#define NO_WAIT						1
#define WAIT_DIE					2
#define DL_DETECT					3
#define TIMESTAMP					4
#define MVCC						5
#define HSTORE						6
#define OCC							7
#define TICTOC						8
#define SILO						9
#define VLL							10
#define HEKATON 					11
#define WOUND_WAIT                  12
#define BAMBOO                      13
#define IC3                         14
#define SILO_PRIO					15
#define ARIA						16
//Isolation Levels
#define SERIALIZABLE				1
#define SNAPSHOT					2
#define REPEATABLE_READ				3
// TIMESTAMP allocation method.
#define TS_MUTEX					1
#define TS_CAS						2
#define TS_HW						3
#define TS_CLOCK					4
// Synthetic YCSB - HOTSPOT POSITION
#define TOP                         1
#define MID                         2
#define BOT                         3
#define TM                          4
#define MB                          5
#define SPECIFIED                   6

#endif
