import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# https://stackoverflow.com/a/31575603
from matplotlib.ticker import FixedFormatter, FixedLocator
from matplotlib import transforms as mtransforms
from matplotlib import scale as mscale
from numpy import ma

# based on: https://colorbrewer2.org/#type=qualitative&scheme=Set3&n=5
# this color map ensure the curves are still readable in grayscale
color_map = {
    "NO_WAIT": "#8dd3c7",
    "WAIT_DIE": "#80b1d3",
    "WOUND_WAIT": "#bebada",
    "SILO": "#fdaa48",
    "SILO_PRIO": "#fb8072",
    # for exec/abort time
    "exec_time": "#b2df8a",
    "abort_time": "#fb9a99",
}

# linestyle and marker are unused if making bar graph instead of plot
linestyle_map = {
    "NO_WAIT": "-",
    "WAIT_DIE": "-",
    "WOUND_WAIT": "-",
    "SILO": "-",
    "SILO_PRIO": "-",
}

marker_map = {
    "NO_WAIT": "^",
    "WAIT_DIE": "s",
    "WOUND_WAIT": "d",
    "SILO": "x",
    "SILO_PRIO": "o",
}

marker_size = 3

FIG_SIZE = (5, 2.7)


def plot_throughput_vs_thread(exper: str):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 2, 4, 8, 16, 32, 64]
    tp_ticks = list(range(0, 1000001, 200000))
    data_path = f"results/{exper}/throughput.csv"

    x = np.arange(len(thread_cnts))
    width = 0.15

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    df = pd.read_csv(data_path, header=0, na_values="None",
                     skipinitialspace=True)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg)]
        ax.bar(x=x + width * (i + 1.5 - len(thread_cnts) / 2),
               height=cc_df["throughput"].to_numpy(),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('# threads')
    ax.set_ylabel('throughput (Mtxn/s)')

    ax.set_xticks(x, list(f"{t}" for t in thread_cnts))
    ax.set_yticks(tp_ticks,
                  list(f"{tp/1000000}" if tp > 0 else "0" for tp in tp_ticks),
                  rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-thread_vs_throughput.pdf")


def plot_throughput_vs_zipf(exper: str, thread_cnt: int = None):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    zipf_theta_list = [0.1, 0.3, 0.5, 0.7, 0.9, 0.99, 1.1, 1.3, 1.5]
    tp_ticks = list(range(0, 1000001, 200000))
    data_path = f"results/{exper}/throughput.csv"

    x = np.arange(len(zipf_theta_list))
    width = 0.15

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    df = pd.read_csv(data_path, header=0, na_values="None",
                     skipinitialspace=True)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg)]
        if thread_cnt is not None:
            cc_df = cc_df[(cc_df["thread_cnt"] == thread_cnt)]
        ax.bar(x=x + width * (i + 2.5 - len(zipf_theta_list) / 2),
               height=cc_df["throughput"].to_numpy(),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('zipf_theta')
    ax.set_ylabel('throughput (Mtxn/s)')

    ax.set_xticks(x, list(f"{t}" for t in zipf_theta_list))
    ax.set_yticks(tp_ticks,
                  list(f"{tp/1000000}" if tp > 0 else "0" for tp in tp_ticks),
                  rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-{thread_cnt}-zipf_vs_throughput.pdf")


def plot_tail_latency_vs_thread(exper: str,
                                metric: str = "p999",
                                tag: str = "all"):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 2, 4, 8, 16, 32, 64]
    latency_ticks = list(range(0, 401, 100))
    data_path = f"results/{exper}/tail.csv"

    x = np.arange(len(thread_cnts))
    width = 0.15

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    df = pd.read_csv(data_path, header=0, na_values="None",
                     skipinitialspace=True)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg) & (df["tag"] == tag)]
        ax.bar(x=x + width * (i + 1.5 - len(thread_cnts) / 2),
               height=cc_df[metric].to_numpy(),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('# threads')
    ax.set_ylabel(f'{metric} tail latency (us)')

    ax.set_xticks(x, list(f"{t}" for t in thread_cnts))
    ax.set_yticks(latency_ticks,
                  list(f"{l}" for l in latency_ticks),
                  rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-{tag}-thread_vs_tail-{metric}.pdf")


def plot_tail_latency_vs_zipf(exper: str,
                              metric: str = "p999",
                              thread_cnt: int = None):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    zipf_theta_list = [0.1, 0.3, 0.5, 0.7, 0.9, 0.99, 1.1, 1.3, 1.5]
    latency_ticks = list(range(0, 401, 100))
    data_path = f"results/{exper}/tail.csv"

    x = np.arange(len(zipf_theta_list))
    width = 0.15

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    df = pd.read_csv(data_path, header=0, na_values="None",
                     skipinitialspace=True)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg) & (df["tag"] == "all")]
        if thread_cnt is not None:
            cc_df = cc_df[(cc_df["thread_cnt"] == thread_cnt)]
        ax.bar(x=x + width * (i + 2.5 - len(zipf_theta_list) / 2),
               height=cc_df[metric].to_numpy(),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('zipf_theta')
    ax.set_ylabel(f'{metric} tail latency (us)')

    ax.set_xticks(x, list(f"{t}" for t in zipf_theta_list))
    ax.set_yticks(latency_ticks,
                  list(f"{l}" for l in latency_ticks),
                  rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-{thread_cnt}-zipf_vs_tail-{metric}.pdf")


def plot_exec_time(exper: str, thread_cnt=64):
    time_bars = ["exec_time", "abort_time"]
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    data_path = f"results/{exper}/prio.csv"

    x = np.arange(len(cc_algs))
    width = 0.3

    fig, ax = plt.subplots(figsize=FIG_SIZE)
    df = pd.read_csv(data_path, header=0, na_values="None",
                     skipinitialspace=True)

    height = {t: [] for t in time_bars}

    for cc_alg in cc_algs:
        for time_bar in time_bars:
            cc_df = df[(df["thread_cnt"] == thread_cnt)
                       & (df["cc_alg"] == cc_alg)]
            cc_df = cc_df.groupby(['thread_cnt', 'zipf_theta', 'cc_alg']).sum()
            # first draw exec_time
            time_tmp = cc_df[time_bar].to_numpy()
            assert len(time_tmp) == 1
            cnt_tmp = cc_df["txn_cnt"].to_numpy()
            assert len(cnt_tmp) == 1
            height[time_bar].append(time_tmp[0] / cnt_tmp[0])

    scale_base = height["exec_time"][-1]  # SILO_PRIO's exec_time

    ax.bar(x=x,
           height=np.array(height["exec_time"]) / scale_base,
           width=width,
           color=color_map['exec_time'],
           label='exec_time')

    ax.bar(x=x,
           height=np.array(height["abort_time"]) / scale_base,
           width=width,
           bottom=np.array(height["exec_time"]) / scale_base,
           color=color_map['abort_time'],
           label='abort_time')

    ax.set_xticks(x, cc_algs)
    ax.set_yticks(range(0, 5, 1), rotation=90)

    ax.set_xlabel('concurrency control algorithm')
    ax.set_ylabel(f'scale')

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-{thread_cnt}-thread_vs_exec.pdf")


class CloseToOne(mscale.ScaleBase):
    name = 'close_to_one'

    def __init__(self, axis, **kwargs):
        super().__init__(axis)
        self.nines = kwargs.get('nines', 4)

    def get_transform(self):
        return self.Transform(self.nines)

    def set_default_locators_and_formatters(self, axis):
        axis.set_major_locator(FixedLocator(
            np.array([1-10**(-k) for k in range(1+self.nines)])))
        axis.set_major_formatter(FixedFormatter(
            [f"p{''.join('9' * k)}" if k > 1 else "p90" if k > 0 else '0' for k in range(1+self.nines)]))

    def limit_range_for_scale(self, vmin, vmax, minpos):
        return 0, min(1 - 10**(-self.nines), vmax)

    class Transform(mtransforms.Transform):
        input_dims = 1
        output_dims = 1
        is_separable = True

        def __init__(self, nines):
            mtransforms.Transform.__init__(self)
            self.nines = nines

        def transform_non_affine(self, a):
            masked = ma.masked_where(a > 1-10**(-1-self.nines), a)
            if masked.mask.any():
                return -ma.log10(1-a)
            else:
                return -np.log10(1-a)

        def inverted(self):
            return CloseToOne.InvertedTransform(self.nines)

    class InvertedTransform(mtransforms.Transform):
        input_dims = 1
        output_dims = 1
        is_separable = True

        def __init__(self, nines):
            mtransforms.Transform.__init__(self)
            self.nines = nines

        def transform_non_affine(self, a):
            return 1. - 10**(-a)

        def inverted(self):
            return CloseToOne.Transform(self.nines)


mscale.register_scale(CloseToOne)


def plot_latency_logscale(exper: str, thread_cnt=64, zipf=0.9):
    fig, ax = plt.subplots(figsize=FIG_SIZE)

    for cc_alg in ["SILO"]:
        data_path = f"results/{exper}/YCSB-CC={cc_alg}-THD={thread_cnt}-ZIPF={zipf}/latency_dump.csv"
        df = pd.read_csv(data_path, header=0, names=[
            'prio', 'latency'], na_values="None", skipinitialspace=True)
        latency = df['latency'].to_numpy()
        latency.sort()
        latency = latency / 1000
        p = np.arange(len(latency)) / len(latency)

        ax.plot(latency, p, color=color_map[cc_alg],
                linestyle=linestyle_map[cc_alg], label=cc_alg)

    # then SILO_PRIO
    data_path = f"results/{exper}/YCSB-CC=SILO_PRIO-THD={thread_cnt}-ZIPF={zipf}/latency_dump.csv"
    df = pd.read_csv(data_path, header=0, names=[
        'prio', 'latency'], na_values="None", skipinitialspace=True)

    # prio=0
    latency_p0 = df[(df['prio'] == 0)]['latency'].to_numpy()
    latency_p0.sort()
    latency_p0 = latency_p0 / 1000
    p = np.arange(len(latency_p0)) / len(latency_p0)
    ax.plot(latency_p0, p, label='SILO_PRIO:low')

    # prio=1
    latency_p1 = df[(df['prio'] == 1)]['latency'].to_numpy()
    latency_p1.sort()
    latency_p1 = latency_p1 / 1000
    p = np.arange(len(latency_p1)) / len(latency_p1)
    ax.plot(latency_p1, p, label='SILO_PRIO:high')

    ax.set_xlabel('latency (us)')
    # ax.set_ylabel(f'tail percentage (%)')

    ax.set_xlim(0, 2000)

    ax.set_yscale('close_to_one')
    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-{thread_cnt}-latency_logscale.pdf")


plot_throughput_vs_thread("autoprio_thd")
plot_throughput_vs_zipf("autoprio_zipf", 32)
plot_throughput_vs_zipf("autoprio_zipf", 64)
plot_tail_latency_vs_thread("autoprio_thd", "p999")
plot_tail_latency_vs_zipf("autoprio_zipf", "p999", 64)
plot_exec_time("autoprio_thd", 64)
plot_latency_logscale("fixedprio_binary_thd")
