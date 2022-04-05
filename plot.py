import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List, Dict

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

label_map = {
    "NO_WAIT": "NO_WAIT",
    "WAIT_DIE": "WAIT_DIE",
    "WOUND_WAIT": "WOUND_WAIT",
    "SILO": "SILO",
    "SILO_PRIO": "SILO_PRIO",
}

marker_size = 3

FIG_SIZE = (5, 2.7)


def set_fig(fig, fig_size=FIG_SIZE):
    # handle all figure parameters tuning
    fig.set_tight_layout({"pad": 0.1, "w_pad": 0.5, "h_pad": 0.5})
    fig.set_size_inches(*fig_size)


def get_subplots_2UD(sharex=True, fig_size=FIG_SIZE):
    fig, axes = plt.subplots(nrows=2, ncols=1, sharex=sharex)
    set_fig(fig, fig_size=fig_size)
    return fig, axes


def get_subplots_2LR(sharex=True, fig_size=FIG_SIZE):
    fig, axes = plt.subplots(nrows=1, ncols=2, sharex=sharex)
    set_fig(fig, fig_size=fig_size)
    return fig, axes


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
            return -ma.log10(1-a) if masked.mask.any() else -np.log10(1-a)

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


def make_subplot(df: pd.DataFrame, ax, x_col: str, y_col: str, z_col: str, x_range: List[int], z_range: List,
                 filters: Dict):
    filter_df = df
    for fk, fv in filters.items():
        filter_df = filter_df[(filter_df[fk] == fv)]

    # each z corresponds to a legend
    for z_val in z_range:
        z_df = filter_df[(filter_df[z_col] == z_val)]
        y_data = []
        for x_val in x_range:
            d = z_df[(z_df[x_col] == x_val)]
            if d.shape[0] != 1:
                print(f"{x_val}, {z_val}")
            assert d.shape[0] == 1
            y_data.append(d.head(1)[y_col])
        ax.plot(x_range, y_data,
                color=color_map[z_val],
                marker=marker_map[z_val],
                markersize=marker_size,
                label=label_map[z_val])


def plot_thread_vs_throughput_tail(exper: str, tail_metric='p999', layout='LR'):
    assert layout in {"LR", "UD"}
    fig, axes = get_subplots_2LR() if layout == 'LR' else get_subplots_2UD()
    ax_tp, ax_tail = axes

    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 2, 4, 8, 16, 24, 32, 40, 48, 56, 64]

    # plot throughput
    tp_df = pd.read_csv(f"results/{exper}/throughput.csv", header=0,
                        na_values="None", skipinitialspace=True)
    make_subplot(df=tp_df, ax=ax_tp, x_col='thread_cnt', y_col='throughput',
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"zipf_theta": 0.99})

    # plot tail latency
    tail_df = pd.read_csv(f"results/{exper}/tail.csv", header=0,
                          na_values="None", skipinitialspace=True)
    make_subplot(df=tail_df, ax=ax_tail, x_col='thread_cnt', y_col=tail_metric,
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"zipf_theta": 0.99, 'tag': 'all'})

    ax_tp.set_ylabel('Throughput (Mtxn/s)')
    ax_tail.set_ylabel(f'{tail_metric} (ms)')
    ax_tp.set_xlabel('Number of threads')
    ax_tail.set_xlabel('Number of threads')

    ax_tp.set_xticks([1, 8, 16, 32, 48, 64])
    ax_tail.set_xticks([1, 8, 16, 32, 48, 64])
    ax_tp.set_xlim(0)
    ax_tail.set_xlim(0)

    tp_ticks = list(range(0, 1000001, 200000))
    ax_tp.set_yticks(
        tp_ticks, [f"{t/1000000}" if t > 0 else "0" for t in tp_ticks], rotation=90)
    ax_tp.set_ylim([0, 1000000])

    tail_ticks = list(range(0, 1251, 250))
    ax_tail.set_yticks(
        tail_ticks, [f"{t/1000}" if t > 0 else "0" for t in tail_ticks], rotation=90)
    ax_tail.set_ylim([0, 1250])

    fig.savefig(f"{exper}-thread_vs_throughput_tail.pdf")


def plot_zipf_vs_throughput_tail(exper: str, tail_metric='p999', layout='LR'):
    assert layout in {"LR", "UD"}
    fig, axes = get_subplots_2LR() if layout == 'LR' else get_subplots_2UD()
    ax_tp, ax_tail = axes

    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    zipf_thetas = [0.9, 0.99, 1.1, 1.2, 1.3, 1.4, 1.5]

    # plot throughput
    tp_df = pd.read_csv(f"results/{exper}/throughput.csv", header=0,
                        na_values="None", skipinitialspace=True)
    make_subplot(df=tp_df, ax=ax_tp, x_col='zipf_theta', y_col='throughput',
                 z_col='cc_alg', x_range=zipf_thetas, z_range=cc_algs,
                 filters={"thread_cnt": 64})

    # plot tail latency
    tail_df = pd.read_csv(f"results/{exper}/tail.csv", header=0,
                          na_values="None", skipinitialspace=True)
    make_subplot(df=tail_df, ax=ax_tail, x_col='zipf_theta', y_col=tail_metric,
                 z_col='cc_alg', x_range=zipf_thetas, z_range=cc_algs,
                 filters={"thread_cnt": 64, 'tag': 'all'})

    ax_tp.set_ylabel('Throughput (Mtxn/s)')
    ax_tail.set_ylabel(f'{tail_metric} (ms)')
    ax_tp.set_xlabel('Zipfian theta')
    ax_tail.set_xlabel('Zipfian theta')

    zipf_ticks = [0.9, 0.99, 1.1, 1.3, 1.5]
    ax_tp.set_xticks(
        zipf_ticks, [f"{t:.1f}" if t != 0.99 else f"{t:.2f}" for t in zipf_ticks])
    ax_tail.set_xticks(zipf_thetas)

    tp_ticks = list(range(0, 1000001, 200000))
    ax_tp.set_yticks(
        tp_ticks, [f"{t/1000000}" if t > 0 else "0" for t in tp_ticks], rotation=90)
    ax_tp.set_ylim([0, 1000000])

    tail_ticks = list(range(0, 2501, 500))
    ax_tail.set_yticks(
        tail_ticks, [f"{t/1000}" if t > 0 else "0" for t in tail_ticks], rotation=90)
    ax_tail.set_ylim([0, 2500])

    fig.savefig(f"{exper}-zipf_vs_throughput_tail.pdf")


def plot_latency_logscale(exper: str, thread_cnt=64, zipf=0.99):
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


plot_thread_vs_throughput_tail("autoprio_thd")
plot_zipf_vs_throughput_tail("autoprio_zipf")
plot_thread_vs_throughput_tail("autoprio_longtxn")
