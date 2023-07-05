#!/usr/bin/env python3
import os
import re
import sys
import os.path
from typing import List


class DataPoint():
    # regex for directory name, which encoding experiment metadata
    re_dirname = re.compile(
        r'\A(?P<wl>[A-Z]+)-CC=(?P<cc_alg>[A-Z_0-9]+)-THD=(?P<thread_cnt>[0-9]+)(-ZIPF=(?P<zipf_theta>[0-9.]+))?(-NUM_WH=(?P<num_wh>[0-9]+))?(-PRIO_RATIO=(?P<prio_ratio>[0-9.]+))?\Z')
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
        self.wl = d['wl']
        assert self.wl in {"YCSB", "TPCC"}
        self.params = d  # cc_alg, thread_cnt, zipf_theta, num_wh, prio_ratio
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

    def get_base_header(self) -> List[str]:
        return [
            p for p in ["cc_alg", "thread_cnt", "zipf_theta", "num_wh", "prio_ratio"]
            if self.params.get(p)
        ]

    def get_base_data(self) -> List:
        return [
            self.params.get(p) for p in ["cc_alg", "thread_cnt", "zipf_theta", "num_wh", "prio_ratio"]
            if self.params.get(p)
        ]

    def get_throughput_header(self) -> List[str]:
        h = self.get_base_header()
        h.append("throughput")
        return h

    def get_throughput_data(self) -> List[str]:
        d = self.get_base_data()
        d.append(str(self.throughput))
        return d

    def get_tail_header(self) -> List[str]:
        h = self.get_base_header()
        h.extend(['tag', 'p50', 'p99', 'p999', 'p9999'])
        return h

    def get_tail_data(self) -> List[List]:
        d = self.get_base_data()
        return [
            d + [tag, str(tail['p50']), str(tail['p99']),
                 str(tail['p999']), str(tail['p9999'])]
            for tag, tail in self.tail.items()
        ]


def parse_datapoint(prefix: str, dirname: str) -> DataPoint:
    if dirname.startswith("YCSB") or dirname.startswith("TPCC"):
        return DataPoint(prefix, dirname)
    else:
        print(f"Unknown experiment: {dirname}")


def dump_throughput(datapoints: List[DataPoint], path: str, has_header: bool = True):
    with open(path, 'w') as f:
        if has_header:
            f.write(f"{','.join(datapoints[0].get_throughput_header())}\n")
        for dp in datapoints:
            f.write(f"{','.join(dp.get_throughput_data())}\n")


def dump_tail(datapoints: List[DataPoint], path: str, has_header: bool = True):
    with open(path, 'w') as f:
        if has_header:
            if has_header:
                f.write(f"{','.join(datapoints[0].get_tail_header())}\n")
            for dp in datapoints:
                for l in dp.get_tail_data():
                    f.write(f"{','.join(l)}\n")


if __name__ == "__main__":
    if len(sys.argv) <= 1:
        print(f"Usage: {sys.argv[0]} exper1 [exper2 [exper3...]]")
    exper_list = sys.argv[1:]

    for exper in exper_list:
        dp_list = [
            parse_datapoint(f'results/{exper}', d)
            for d in os.listdir(f'results/{exper}')
            if os.path.isdir(f'results/{exper}/{d}')
        ]
        dump_throughput(dp_list, f'results/{exper}/throughput.csv')
        dump_tail(dp_list, f'results/{exper}/tail.csv')
