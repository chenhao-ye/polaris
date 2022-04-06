import matplotlib.pyplot as plt
import pandas as pd
import numpy as np
from typing import List, Dict, Tuple, Optional

# https://stackoverflow.com/a/31575603
from matplotlib.ticker import FixedFormatter, FixedLocator
from matplotlib import transforms as mtransforms
from matplotlib import scale as mscale
from numpy import ma

# "pdf", "eps", "png", etc
IMAGE_TYPE = "pdf"

# based on: https://colorbrewer2.org/#type=qualitative&scheme=Set3&n=5
# this color map ensure the curves are still readable in grayscale
color_map = {
    "NO_WAIT": "#bebada",
    "WAIT_DIE": "#80b1d3",
    "WOUND_WAIT": "#8dd3c7",
    "SILO": "#fdaa48",
    "SILO_PRIO": "#fb8072",
    # log scale plot
    "SILO_PRIO_FIXED": "#8dd3c7",
    "SILO_PRIO:High": "#fb8072",
    "SILO_PRIO:Low": "#fb8072",
    "SILO_PRIO_FIXED:High": "#8dd3c7",
    "SILO_PRIO_FIXED:Low": "#8dd3c7",
}

# linestyle and marker are unused if making bar graph instead of plot
linestyle_map = {
    "NO_WAIT": "-",
    "WAIT_DIE": "-",
    "WOUND_WAIT": "-",
    "SILO": "-",
    "SILO_PRIO": "-",
    # log scale plot:
    "SILO_PRIO:High": "-",
    "SILO_PRIO:Low": "--",
    "SILO_PRIO_FIXED:High": "-",
    "SILO_PRIO_FIXED:Low": "--",
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
    "SILO_PRIO_FIXED": "SILO_PRIO_FIXED",
    "SILO_PRIO:High": "SILO_PRIO:High",
    "SILO_PRIO:Low": "SILO_PRIO:Low",
    "SILO_PRIO_FIXED:High": "SILO_PRIO_FIXED:High",
    "SILO_PRIO_FIXED:Low": "SILO_PRIO_FIXED:Low",
}

marker_size = 3

FIG_SIZE = (5, 2.5)


def set_fig(fig, fig_size=FIG_SIZE):
    # handle all figure parameters tuning
    fig.set_tight_layout({"pad": 0.01, "w_pad": 0.5, "h_pad": 0})
    fig.set_size_inches(*fig_size)


def get_subplots_UD(fig_size=FIG_SIZE):
    fig, axes = plt.subplots(nrows=2, ncols=1)
    set_fig(fig, fig_size=fig_size)
    return fig, axes


def get_subplots_LR(fig_size=FIG_SIZE):
    fig, axes = plt.subplots(nrows=1, ncols=2)
    set_fig(fig, fig_size=fig_size)
    return fig, axes


def get_subplots_2L1R(fig_size=FIG_SIZE):
    fig = plt.figure()
    ax_l = fig.add_subplot(1, 3, (1, 2))
    ax_r = fig.add_subplot(1, 3, 3)
    set_fig(fig, fig_size=fig_size)
    return fig, (ax_l, ax_r)


def make_subplot(df: pd.DataFrame, ax, x_col: str, y_col: str, z_col: str,
                 x_range: List[int], z_range: List, filters: Dict):
    filter_df = df
    for fk, fv in filters.items():
        filter_df = filter_df[(filter_df[fk] == fv)]

    # each z corresponds to a legend
    for z_val in z_range:
        z_df = filter_df[(filter_df[z_col] == z_val)]
        y_data = []
        for x_val in x_range:
            d = z_df[(z_df[x_col] == x_val)]
            assert d.shape[0] == 1
            y_data.append(d.head(1)[y_col])
        ax.plot(x_range, y_data,
                color=color_map[z_val],
                marker=marker_map[z_val],
                markersize=marker_size,
                label=label_map[z_val])


def make_tail_latency_subplot(df: pd.DataFrame, ax, z_col: str,
                              prio_range: Optional[Tuple[int, int]] = None):
    filtered_df = df
    if prio_range:
        prio_min, prio_max = prio_range
        filtered_df = df[(df['prio'] >= prio_min) & (df['prio'] <= prio_max)]
    latency = filtered_df['latency'].to_numpy()
    latency.sort()
    latency = latency / 1e6
    p = np.arange(len(latency)) / len(latency)

    ax.plot(latency, -np.log10(1 - p), color=color_map[z_col],
            linestyle=linestyle_map[z_col], label=label_map[z_col])


def plot_thread_vs_throughput_tail(exper: str, tail_metric='p999', layout='LR'):
    assert layout in {"LR", "UD"}
    fig, (ax_tp, ax_tail) = get_subplots_LR() \
        if layout == 'LR' else get_subplots_UD()

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

    ax_tp.set_ylabel('Throughput (million txn/s)')
    ax_tail.set_ylabel(f'Tail latency {tail_metric} (ms)')
    ax_tp.set_xlabel('Number of threads')
    ax_tail.set_xlabel('Number of threads')

    ax_tp.set_xticks([1, 8, 16, 32, 48, 64])
    ax_tail.set_xticks([1, 8, 16, 32, 48, 64])
    ax_tp.set_xlim(0)
    ax_tail.set_xlim(0)

    return fig, (ax_tp, ax_tail)


def plot_zipf_vs_throughput_tail(exper: str, tail_metric='p999', layout='LR'):
    assert layout in {"LR", "UD"}
    fig, (ax_tp, ax_tail) = get_subplots_LR() \
        if layout == 'LR' else get_subplots_UD()

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

    zipf_ticks = zipf_thetas
    ax_tp.set_xticks(
        zipf_ticks, [f"{t:.1f}" if t != 0.99 else f"{t:.2f}" for t in zipf_ticks])
    ax_tail.set_xticks(
        zipf_ticks, [f"{t:.1f}" if t != 0.99 else f"{t:.2f}" for t in zipf_ticks])

    tp_ticks = list(range(0, 600001, 150000))
    ax_tp.set_yticks(
        tp_ticks, [f"{t/1e6}" if t > 0 else "0" for t in tp_ticks], rotation=90)
    ax_tp.set_ylim([0, 600000])

    tail_ticks = list(range(0, 16001, 4000))
    ax_tail.set_yticks(
        tail_ticks, [f"{t//1000}" if t > 0 else "0" for t in tail_ticks], rotation=90)
    ax_tail.set_ylim([0, 16000])

    ax_tp_zoom = ax_tp.inset_axes([0.45, 0.45, 0.5, 0.5])
    make_subplot(df=tp_df, ax=ax_tp_zoom, x_col='zipf_theta', y_col='throughput',
                 z_col='cc_alg', x_range=[1.2, 1.3, 1.4, 1.5], z_range=cc_algs,
                 filters={"thread_cnt": 64})
    tp_ticks_zoom = list(range(0, 160001, 80000))
    ax_tp_zoom.set_xticks([1.2, 1.3, 1.4, 1.5])
    ax_tp_zoom.set_yticks(
        tp_ticks_zoom, [f"{t/1e6}" if t > 0 else "0" for t in tp_ticks_zoom], rotation=90)
    ax_tp_zoom.set_ylim([0, 160000])

    ax_tp.set_ylabel('Throughput (million txn/s)')
    ax_tail.set_ylabel(f'Tail latency {tail_metric} (ms)')
    ax_tp.set_xlabel('Zipfian theta')
    ax_tail.set_xlabel('Zipfian theta')

    return fig, (ax_tp, ax_tail)


def plot_latency_logscale_throughput(exper: str, thread_cnt=64, zipf=0.99):
    fig, (ax_tail, ax_tp) = get_subplots_2L1R()

    # SILO
    cc_alg = "SILO"
    data_path = f"results/{exper}/YCSB-CC={cc_alg}-THD={thread_cnt}-ZIPF={zipf}/latency_dump.csv"
    df = pd.read_csv(data_path, header=0, names=['prio', 'latency'],
                     na_values="None", skipinitialspace=True)
    make_tail_latency_subplot(df, ax_tail, "SILO")

    # then SILO_PRIO_FIXED
    cc_alg = "SILO_PRIO_FIXED"
    data_path = f"results/{exper}/YCSB-CC={cc_alg}-THD={thread_cnt}-ZIPF={zipf}/latency_dump.csv"
    df = pd.read_csv(data_path, header=0, names=['prio', 'latency'],
                     na_values="None", skipinitialspace=True)
    make_tail_latency_subplot(df, ax_tail, f"{cc_alg}:High", [8, 15])
    make_tail_latency_subplot(df, ax_tail, f"{cc_alg}:Low", [0, 7])

    # finally, SILO_PRIO
    cc_alg = "SILO_PRIO"
    data_path = f"results/{exper}/YCSB-CC={cc_alg}-THD={thread_cnt}-ZIPF={zipf}/latency_dump.csv"
    df = pd.read_csv(data_path, header=0, names=['prio', 'latency'],
                     na_values="None", skipinitialspace=True)
    make_tail_latency_subplot(df, ax_tail, f"{cc_alg}:High", [8, 15])
    make_tail_latency_subplot(df, ax_tail, f"{cc_alg}:Low", [0, 7])

    ax_tail.grid(True, axis='y', linestyle='--', linewidth=0.1)

    ax_tail.set_xlim(0, 3)
    ax_tail.set_ylim(0, 4)

    ax_tail.set_yticks([0, 1, 2, 3, 4], ["0", "p90", "p99", "p999", "p9999"],
                       rotation=90)

    ax_tail.set_xlabel("Latency (ms)")
    ax_tail.set_ylabel(f"Tail percentage")

    # then draw bar-graph for throughput
    tp_df = pd.read_csv(f"results/{exper}/throughput.csv", header=0,
                        na_values="None", skipinitialspace=True)
    for i, cc_alg in enumerate(["SILO", "SILO_PRIO_FIXED", "SILO_PRIO"]):
        d = tp_df[(tp_df["cc_alg"] == cc_alg)]
        assert d.shape[0] == 1
        ax_tp.bar(i, d.head(1)["throughput"],
                  width=0.6, color=color_map[cc_alg], label=label_map[cc_alg])

    # ax_tp.set_xticks([0, 1, 2], ["SILO", "SILO_PRIO_FIXED", "SILO_PRIO"], rotation=45)
    plt.xticks([], [])
    tp_ticks = list(range(0, 600001, 150000))
    ax_tp.set_yticks(tp_ticks, [f"{t / 1e6}" for t in tp_ticks], rotation=90)
    ax_tp.set_ylim([0, 600000])

    ax_tp.set_xlabel('Algorithm')
    ax_tp.set_ylabel('Throughput (million txn/s)')

    return fig, (ax_tail, ax_tp)


def plot1():
    fig, (ax_tp, ax_tail) = plot_thread_vs_throughput_tail("autoprio_thd")

    tp_ticks = list(range(0, 600001, 150000))
    ax_tp.set_yticks(
        tp_ticks, [f"{t/1e6}" if t > 0 else "0" for t in tp_ticks], rotation=90)
    ax_tp.set_ylim([0, 600000])

    tail_ticks = list(range(0, 1601, 400))
    ax_tail.set_yticks(
        tail_ticks, [f"{t/1000}" if t > 0 else "0" for t in tail_ticks], rotation=90)
    ax_tail.set_ylim([0, 1600])

    fig.savefig(f"thread_vs_throughput_tail.{IMAGE_TYPE}", transparent=True)


def plot2():
    fig, (ax_tp, ax_tail) = plot_zipf_vs_throughput_tail("autoprio_zipf")
    fig.savefig(f"zipf_vs_throughput_tail.{IMAGE_TYPE}",
                transparent=True)


def plot3():
    fig, (ax_tp, ax_tail) = plot_thread_vs_throughput_tail("autoprio_longtxn")

    tp_ticks = list(range(0, 160001, 40000))
    ax_tp.set_yticks(
        tp_ticks, [f"{t/1e6}" if t > 0 else "0" for t in tp_ticks], rotation=90)
    ax_tp.set_ylim([0, 160000])

    tail_ticks = list(range(0, 16001, 4000))
    ax_tail.set_yticks(
        tail_ticks, [f"{t/1000}" if t > 0 else "0" for t in tail_ticks], rotation=90)
    ax_tail.set_ylim([0, 16000])

    fig.savefig(
        f"longtxn_thread_vs_throughput_tail.{IMAGE_TYPE}", transparent=True)


def plot4():
    fig, (ax_tail, ax_tp) = plot_latency_logscale_throughput("udprio")
    fig.savefig(f"latency_logscale_throughput.{IMAGE_TYPE}")


plot1()
plot2()
plot3()
plot4()
