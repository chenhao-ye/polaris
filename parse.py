#!/usr/bin/env python3
import os
import re
import sys
import os.path
from typing import List


class DataPoint():
    # regex for directory name, which encoding experiment metadata
    # if YCSB, ZIPF is a float-point number; if TPCC, ZIPF="X"
    re_dirname = re.compile(
        r'\AYCSB-CC=(?P<cc_alg>[A-Z_]+)-THD=(?P<thread_cnt>[0-9]+)-ZIPF=(?P<zipf_theta>[0-9.X]+)\Z')
    # regex to filter throughput
    re_throughput = re.compile(
        r'\A\[summary\] throughput=(?P<throughput>[0-9.e+]+),')
    # regex to filter tail latency
    re_tail = re.compile(
        r'\A\[(?P<tag>\S+):tail\]\s+txn_cnt=(?P<txn_cnt>[0-9]+)(, p50=(?P<p50>[0-9.]+))?(, p90=(?P<p90>[0-9.]+))?(, p99=(?P<p99>[0-9.]+))?(, p999=(?P<p999>[0-9.]+))?(, p9999=(?P<p9999>[0-9.]+))?')
    # regex to filter per-priority breakdown
    re_prio_breakdown = re.compile(
        r'\A\[prio=(?P<prio>\d+)\]\s+txn_cnt=(?P<txn_cnt>[0-9]+), abort_cnt=(?P<abort_cnt>[0-9]+), abort_time=(?P<abort_time>[0-9]+), exec_time=(?P<exec_time>[0-9]+), backoff_time=(?P<backoff_time>[0-9]+), ')

    def __init__(self, prefix: str, dirname: str) -> None:
        d = self.re_dirname.match(dirname).groupdict()
        self.cc_alg = d['cc_alg']
        self.thread_cnt = d['thread_cnt']
        self.zipf_theta = d['zipf_theta']
        self.throughput = None
        self.tail = {}
        self.prio_breakdown = {}
        with open(os.path.join(prefix, dirname, "log"), 'r') as f:
            for line in f:
                if line.startswith('[summary]'):
                    self.throughput = self.re_throughput.match(line).groupdict()[
                        'throughput']
                else:
                    m = self.re_tail.match(line)
                    if m is not None:
                        d = m.groupdict()
                        self.tail[d['tag']] = {
                            k: v for k, v in d.items() if k != 'tag'}
                    m = self.re_prio_breakdown.match(line)
                    if m is not None:
                        d = m.groupdict()
                        self.prio_breakdown[d['prio']] = {
                            k: v for k, v in d.items() if k != 'prio'}


def parse_datapoint(prefix: str, dirname: str) -> DataPoint:
    if dirname.startswith("YCSB") or dirname.startswith("TPCC"):
        return DataPoint(prefix, dirname)
    else:
        print(f"Unknown experiment: {dirname}")


def dump_throughput(datapoints: List[DataPoint], path: str, has_header: bool = True):
    with open(path, 'w') as f:
        sep = ', ' if path.endswith('.csv') else '\t'
        if has_header:
            if not path.endswith('.csv'):
                f.write('# ')
            f.write(f"cc_alg{sep}thread_cnt{sep}zipf_theta{sep}throughput\n")
            for dp in datapoints:
                f.write(
                    f"{dp.cc_alg}{sep}{dp.thread_cnt}{sep}{dp.zipf_theta}{sep}{dp.throughput}\n")


def dump_tail(datapoints: List[DataPoint], path: str, has_header: bool = True):
    tail_metrics = ['p50', 'p99', 'p999', 'p9999']
    with open(path, 'w') as f:
        sep = ', ' if path.endswith('.csv') else '\t'
        if has_header:
            if not path.endswith('.csv'):
                f.write('# ')
            f.write(
                f"cc_alg{sep}thread_cnt{sep}zipf_theta{sep}tag{sep}{tail_metrics[0]}")
            for m in tail_metrics[1:]:
                f.write(f"{sep}{m}")
            f.write("\n")
            for dp in datapoints:
                for tag, tail in dp.tail.items():
                    f.write(
                        f"{dp.cc_alg}{sep}{dp.thread_cnt}{sep}{dp.zipf_theta}{sep}{tag}{sep}{tail.get(tail_metrics[0])}")
                    for m in tail_metrics[1:]:
                        f.write(f"{sep}{tail.get(m)}")
                    f.write("\n")


def dump_prio_breakdown(datapoints: List[DataPoint], path: str, has_header: bool = True):
    prio_metrics = ['txn_cnt', 'abort_cnt',
                    'abort_time', 'exec_time', 'backoff_time']
    with open(path, 'w') as f:
        sep = ', ' if path.endswith('.csv') else '\t'
        if has_header:
            if not path.endswith('.csv'):
                f.write('# ')
            f.write(
                f"cc_alg{sep}thread_cnt{sep}zipf_theta{sep}prio{sep}{prio_metrics[0]}")
            for m in prio_metrics[1:]:
                f.write(f"{sep}{m}")
            f.write("\n")
            for dp in datapoints:
                for prio, prio_metric in dp.prio_breakdown.items():
                    f.write(
                        f"{dp.cc_alg}{sep}{dp.thread_cnt}{sep}{dp.zipf_theta}{sep}{prio}{sep}{prio_metric.get(prio_metrics[0])}")
                    for m in prio_metrics[1:]:
                        f.write(f"{sep}{prio_metric.get(m)}")
                    f.write("\n")


if __name__ == "__main__":
    if len(sys.argv) > 1:
        exper_list = sys.argv[1:]
    else:
        exper_list = ['autoprio_thd', 'autoprio_zipf',
                      'autoprio_longtxn', 'fixedprio_binary_thd']

    for exper in exper_list:
        dp_list = []
        for d in os.listdir(f'results/{exper}'):
            if os.path.isdir(f'results/{exper}/{d}'):
                dp_list.append(parse_datapoint(f'results/{exper}', d))
        # the output file can be ".csv" or other
        # if use ".csv", it will use ", " as separator
        # otherwise it will use "\t" and prefix the header with "#", which is
        # zplot's input format
        dump_throughput(dp_list, f'results/{exper}/throughput.data')
        dump_tail(dp_list, f'results/{exper}/tail.data')
        dump_prio_breakdown(dp_list, f'results/{exper}/prio.data')
