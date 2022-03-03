#pragma once

#include "config.h"

#define TMP_METRICS(x, y) \
  x(double, time_wait) x(double, time_man) x(double, time_index)
#define ALL_METRICS(x, y, z) \
  y(uint64_t, txn_cnt) y(uint64_t, abort_cnt) y(uint64_t, user_abort_cnt) \
  x(double, run_time) x(double, time_abort) x(double, time_cleanup) \
  x(double, time_query) x(double, time_get_latch) x(double, time_get_cs) \
  x(double, time_copy) x(double, time_retire_latch) x(double, time_retire_cs) \
  x(double, time_release_latch) x(double, time_release_cs) x(double, time_semaphore_cs) \
  x(double, time_commit) y(uint64_t, time_ts_alloc) y(uint64_t, wait_cnt) \
  y(uint64_t, latency) y(uint64_t, commit_latency) y(uint64_t, abort_length) \
  y(uint64_t, cascading_abort_times) z(uint64_t, max_abort_length) \
  y(uint64_t, txn_cnt_long) y(uint64_t, abort_cnt_long) y(uint64_t, cascading_abort_cnt) \
  y(uint64_t, lock_acquire_cnt) y(uint64_t, lock_directly_cnt) \
  TMP_METRICS(x, y) 
#define DECLARE_VAR(tpe, name) tpe name;
#define INIT_VAR(tpe, name) name = 0;
#define INIT_TOTAL_VAR(tpe, name) tpe total_##name = 0;
#define SUM_UP_STATS(tpe, name) total_##name += _stats[tid]->name;
#define MAX_STATS(tpe, name) total_##name = max(total_##name, _stats[tid]->name);
#define STR(x) #x
#define XSTR(x) STR(x)
#define STR_X(tpe, name) XSTR(name)
#define VAL_X(tpe, name) total_##name / BILLION
#define VAL_Y(tpe, name) total_##name
#define PRINT_STAT_X(tpe, name) \
  std::cout << STR_X(tpe, name) << "= " << VAL_X(tpe, name) << ", ";
#define PRINT_STAT_Y(tpe, name) \
  std::cout << STR_X(tpe, name) << "= " << VAL_Y(tpe, name) << ", ";
#define WRITE_STAT_X(tpe, name) \
  outf << STR_X(tpe, name) << "= " << VAL_X(tpe, name) << ", ";
#define WRITE_STAT_Y(tpe, name) \
  outf << STR_X(tpe, name) << "= " << VAL_Y(tpe, name) << ", ";

// counter-array operators
#define DEF_CNT(tpe, name, size) tpe name[size];
#define INIT_CNT(tpe, name, size) memset(name, 0, (size) * sizeof(tpe)); \
  assert((size) * sizeof(tpe) == sizeof(name));
#define PRINT_CNT(name, size) \
  std::cout << STR(name) << " = [\n"; \
  for (int i = 0; i < (size); ++i) \
    if (name[i]) std::cout << '\t' << i << ": " << name[i] << ',\n'; \
  std::cout << "]\n";
#define INIT_TOTAL_CNT(tpe, name, size) tpe total_##name[(size)] = {};
#define SUM_UP_CNT(name, size) \
  for (int i = 0; i < (size); ++i) \
    total_##name[i] += _stats[tid]->name[i];
#define PRINT_TOTAL_CNT(outf, name, size) \
  outf << STR(name) << " = [\n"; \
  for (int i = 0; i < (size); ++i) \
    if (total_##name[i]) \
      std::cout << '\t' << i << ": " << total_##name[i] << ",\n"; \
  std::cout << "]\n";

union LatencyRecord {
  uint64_t raw_bits;
  struct {
    bool is_long: 1;
    uint32_t abort_cnt : 7;
    uint32_t prio : SILO_PRIO_NUM_BITS_PRIO;
    uint64_t latency : (64 - 1 - 7 - SILO_PRIO_NUM_BITS_PRIO);
  };

  LatencyRecord() = default;
  LatencyRecord(bool is_long, uint32_t abort_cnt, uint32_t prio, uint64_t latency):
    is_long(is_long), abort_cnt(abort_cnt < 128 ? abort_cnt : 127),
    prio(prio), latency(latency) {
    assert (latency < (1 << (64 - 1 - SILO_PRIO_NUM_BITS_PRIO)));
  }

  // `is_long` cannot be both data member name and function name.. so let's call
  // it `get_is_long` for now...
  bool get_is_long() const { return is_long; }
  uint32_t get_abort_cnt() const { return abort_cnt; }
  uint32_t get_prio() const { return prio; }
  uint64_t get_latency() const { return latency; }
};

static_assert(sizeof(LatencyRecord) == 8, "LatencyRecord must be 64-bit");

class Stats_thd {
 public:
  void init(uint64_t thd_id);
  void clear();
  void append_latency(bool is_long, uint32_t abort_cnt, uint32_t prio, uint64_t latency) {
    latency_record[latency_record_len] = {is_long, abort_cnt, prio, latency};
    latency_record_len++;
    assert(latency_record_len <= MAX_TXN_PER_PART);
  }

  char _pad2[CL_SIZE];
  ALL_METRICS(DECLARE_VAR, DECLARE_VAR, DECLARE_VAR)
  LatencyRecord * latency_record;
  uint64_t latency_record_len;

  uint64_t * all_debug1;
  uint64_t * all_debug2;

  // counter for txns for different priorities
  DEF_CNT(uint64_t, prio_txn_cnt, SILO_PRIO_NUM_PRIO_LEVEL);
#if SPLIT_ABORT_COUNT_PRIO
  // counter for high-priority txns (i.e. nonzero priority)
  DEF_CNT(uint64_t, high_prio_abort_txn_cnt, STAT_MAX_NUM_ABORT + 1);
#endif
  // counter for txns with different num_abort (last one for overflow)
  DEF_CNT(uint64_t, abort_txn_cnt, STAT_MAX_NUM_ABORT + 1);

  char _pad[CL_SIZE];
};

class Stats_tmp {
 public:
  void init();
  void clear();
  double time_man;
  double time_index;
  double time_wait;
  char _pad[CL_SIZE - sizeof(double)*3];
};

class Stats {
 public:
  // PER THREAD statistics
  Stats_thd ** _stats;
  // stats are first written to tmp_stats, if the txn successfully commits,
  // copy the values in tmp_stats to _stats
  Stats_tmp ** tmp_stats;

  // GLOBAL statistics
  double dl_detect_time;
  double dl_wait_time;
  uint64_t cycle_detect;
  uint64_t deadlock;

  void init();
  void init(uint64_t thread_id);
  void clear(uint64_t tid);
  void add_debug(uint64_t thd_id, uint64_t value, uint32_t select);
  void commit(uint64_t thd_id);
  void abort(uint64_t thd_id);
  void print();
  void print_lat_distr();
};
