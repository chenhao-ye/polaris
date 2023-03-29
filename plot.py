import matplotlib
import matplotlib.pyplot as plt
import matplotlib.patches as mpatches
import pandas as pd
import numpy as np
import math
from typing import List, Dict, Tuple, Optional

# "pdf", "eps", "png", etc
IMAGE_TYPE = "pdf"
IS_TRANSPARENT = True
TICK_SMALLER_FONTSIZE = 8.5

matplotlib.rcParams['pdf.fonttype'] = 42
matplotlib.rcParams['ps.fonttype'] = 42

# based on: https://colorbrewer2.org/#type=qualitative&scheme=Set3&n=5
# this color map ensure the curves are still readable in grayscale
color_map = {
    "NO_WAIT": "#abdda4",
    "WAIT_DIE": "#abd9e9",
    "WOUND_WAIT": "#2c7bb6",
    "SILO": "#fdae61",
    "SILO_PRIO": "#f03b20",
    "SILO_PRIO:High": "#f03b20",
    "SILO_PRIO:Low": "#f03b20",
    "SILO_PRIO_FIXED": "#ffdd99",
    "SILO_PRIO_FIXED:High": "#ffdd99",
    "SILO_PRIO_FIXED:Low": "#ffdd99",
    "ARIA_1": "#bdd7e7",
    "ARIA_2": "#6baed6",
    "ARIA_4": "#3182bd",
    "ARIA_8": "#08519c",
}

# linestyle and marker are unused if making bar graph instead of plot
linestyle_map = {
    "NO_WAIT": ":",
    "WAIT_DIE": "-.",
    "WOUND_WAIT": "--",
    "SILO": "-",
    "SILO_PRIO": "-",
    "SILO_PRIO:High": "-",
    "SILO_PRIO:Low": "--",
    "SILO_PRIO_FIXED:High": "-",
    "SILO_PRIO_FIXED:Low": "--",
    "ARIA_1": "-",
    "ARIA_2": "--",
    "ARIA_4": "-.",
    "ARIA_8": ":",
}

marker_map = {
    "NO_WAIT": "^",
    "WAIT_DIE": "s",
    "WOUND_WAIT": "d",
    "SILO": "x",
    "SILO_PRIO": "o",
    "SILO_PRIO:High": None,
    "SILO_PRIO:Low": None,
    "SILO_PRIO_FIXED": None,
    "SILO_PRIO_FIXED:High": None,
    "SILO_PRIO_FIXED:Low": None,
    "ARIA_1": "^",
    "ARIA_2": "s",
    "ARIA_4": "d",
    "ARIA_8": "x",
}

label_map = {
    "NO_WAIT": "NO-WAIT",
    "WAIT_DIE": "WAIT-DIE",
    "WOUND_WAIT": "WOUND-WAIT",
    "SILO": "SILO",
    "SILO_PRIO": "POLARIS",
    "SILO_PRIO_FIXED": "POLARIS-SP",
    "SILO_PRIO:High": "POLARIS:High",
    "SILO_PRIO:Low": "POLARIS:Low",
    "SILO_PRIO_FIXED:High": "POLARIS-SP:High",
    "SILO_PRIO_FIXED:Low": "POLARIS-SP:Low",
    "ARIA_1": "ARIA-1",
    "ARIA_2": "ARIA-2",
    "ARIA_4": "ARIA-4",
    "ARIA_8": "ARIA-8",
}

marker_size = 5

SUBFIG_WIDTH = 2.6
SUBFIG_HEIGHT = 2
LEGEND_WIDTH = 6

plt.rcParams['xtick.major.pad'] = '2'
plt.rcParams['ytick.major.pad'] = '2'
plt.rcParams['xtick.major.size'] = '2.5'
plt.rcParams['ytick.major.size'] = '2.5'
plt.rcParams['axes.labelpad'] = '1'
plt.rcParams['figure.max_open_warning'] = '30'


def set_fig(fig, nrows: int, ncols: int):
    # handle all figure parameters tuning
    fig.set_tight_layout({"pad": 0.1, "w_pad": 0.1, "h_pad": 0.1})
    fig.set_size_inches(ncols * SUBFIG_WIDTH, nrows * SUBFIG_HEIGHT)


def get_subplots(nrows, ncols):
    fig, axes = plt.subplots(nrows=nrows, ncols=ncols)
    set_fig(fig=fig, nrows=nrows, ncols=ncols)
    return fig, axes


def get_subplots_2L1R():
    fig = plt.figure()
    ax_l = fig.add_subplot(1, 3, (1, 2))
    ax_r = fig.add_subplot(1, 3, 3)
    set_fig(fig=fig, nrows=1, ncols=3)
    return fig, (ax_l, ax_r)


def load_throughput(exper: str):
    return pd.read_csv(f"results/{exper}/throughput.csv", header=0,
                       na_values="None", skipinitialspace=True)


def load_tail(exper: str):
    return pd.read_csv(f"results/{exper}/tail.csv", header=0,
                       na_values="None", skipinitialspace=True)


def load_latency(exper: str, cc_alg: str, thread_cnt: str, *,
                 zipf: Optional[float] = None, num_wh: Optional[int] = None):
    assert (zipf is None) != (num_wh is None)
    data_path = \
        f"results/{exper}/YCSB-CC={cc_alg}-THD={thread_cnt}-ZIPF={zipf}/latency_dump.csv" \
        if zipf is not None else \
        f"results/{exper}/TPCC-CC={cc_alg}-THD={thread_cnt}-NUM_WH={num_wh}/latency_dump.csv"
    return pd.read_csv(data_path, header=0, names=['prio', 'latency'],
                       na_values="None", skipinitialspace=True)


def set_x_threads(ax, threads: List = [1, 8, 16, 32, 48, 64]):
    ax.set_xlabel("Number of threads")
    ax.set_xticks(threads)
    ax.set_xlim(0)


def set_tp_ax(ax_tp, tick, num_ticks, *, ylabel='Throughput (Mtxn/s)'):
    # tick unit is Mtxn/s
    tp_ticks = [tick * i * 1000000 for i in range(num_ticks + 1)]
    ax_tp.set_yticks(tp_ticks,
                     [f"{tick * i:g}"
                         for i in range(num_ticks + 1)],
                     rotation=90)
    ax_tp.set_ylim([0, tick * num_ticks * 1000000])
    if ylabel:
        ax_tp.set_ylabel(ylabel)


def set_tail_ax(ax_tail, tick, num_ticks, tail_metric='p999'):
    tail_ticks = [tick * i * 1000 for i in range(num_ticks + 1)]
    ax_tail.set_yticks(tail_ticks,
                       [f"{tick * i:g}" if i > 0 else "0"
                        for i in range(num_ticks + 1)],
                       rotation=90)
    ax_tail.set_ylim([0, tick * num_ticks * 1000])
    ax_tail.set_ylabel(f"Tail latency {tail_metric} (ms)")


# this is for latency cdf
def set_lat_ax(ax_lat, tick, num_ticks, xlabel="Latency (ms)", *,
               xlabel_suffix=None):
    lat_ticks = [tick * i * 1000000 for i in range(num_ticks + 1)]
    ax_lat.set_xticks(lat_ticks,
                      [f"{tick * i:g}" if i > 0 else "0"
                       for i in range(num_ticks + 1)])
    ax_lat.set_xlim([0, tick * num_ticks * 1000000])
    ax_lat.set_yticks([-math.log10(0.5), 1, 2, 3, 4],
                      ["p50", "p90", "p99", "p999", "p9999"], rotation=90,
                      fontsize=TICK_SMALLER_FONTSIZE)
    ax_lat.set_ylim([0, 4])
    ax_lat.set_xlabel(
        f"{xlabel}, {xlabel_suffix}" if xlabel_suffix else xlabel)
    ax_lat.set_ylabel("Tail percentage")
    ax_lat.grid(True, axis='y', linestyle='--', linewidth=0.1)


def make_subplot(ax, df: pd.DataFrame, x_col: str, y_col: str, z_col: str,
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
            if d.shape[0] != 1:
                raise ValueError("Unexpected data: "
                                 f"({x_col}={x_val},{z_col}={z_val}): "
                                 f"shape {d.shape}")
            y_data.append(d.head(1)[y_col])
        ax.plot(x_range, y_data,
                color=color_map[z_val],
                linestyle=linestyle_map[z_val],
                marker=marker_map[z_val],
                markersize=marker_size,
                label=label_map[z_val])


def make_cdf(ax, df: pd.DataFrame, z_col: str,
             prio_range: Optional[Tuple[int, int]] = None):
    filtered_df = df
    if prio_range:
        prio_min, prio_max = prio_range
        filtered_df = df[(df['prio'] >= prio_min) & (df['prio'] <= prio_max)]
    latency = filtered_df['latency'].to_numpy()
    latency.sort()
    p = np.arange(len(latency)) / len(latency)

    ax.plot(latency, -np.log10(1 - p), color=color_map[z_col],
            linestyle=linestyle_map[z_col], label=label_map[z_col])


def make_subplot_latency_cdf(ax, dfs: Dict[str, pd.DataFrame],
                             cc_algs: List[str]):
    # this one does not support filtering by prio
    for cc_alg in cc_algs:
        df = dfs[cc_alg]
        make_cdf(ax, df, cc_alg)


def make_bar(ax, df: pd.DataFrame, cc_algs: List[str]):
    for i, cc_alg in enumerate(cc_algs):
        d = df[(df["cc_alg"] == cc_alg)]
        assert d.shape[0] == 1
        ax.bar(i, d.head(1)["throughput"],
               width=0.5, color=color_map[cc_alg],
               label=label_map[cc_alg])

    ax.set_xticks([])
    ax.set_xlabel('Concurrency control algorithm')


def plot_ycsb_thread_vs_throughput_tail(exper: str, tail_metric='p999'):
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        get_subplots(nrows=2, ncols=2)

    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 4, 8, 16, 24, 32, 40, 48, 56, 64]

    # plot throughput
    tp_df = load_throughput(exper)
    make_subplot(ax=ax_tp, df=tp_df, x_col='thread_cnt', y_col='throughput',
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"zipf_theta": 0.99})

    # plot tail latency
    tail_df = load_tail(exper)
    make_subplot(ax=ax_tail, df=tail_df, x_col='thread_cnt', y_col=tail_metric,
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"zipf_theta": 0.99, 'tag': 'all'})

    set_x_threads(ax_tp)
    set_x_threads(ax_tail)

    for thd, ax in zip([16, 64], [ax_lat_l, ax_lat_r]):
        lat_dfs = {cc_alg: load_latency(exper, cc_alg, thd, zipf=0.99)
                   for cc_alg in cc_algs}
        make_subplot_latency_cdf(ax, lat_dfs, cc_algs)

    return fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r))


def plot_ycsb_zipf_vs_throughput_tail(exper: str, zipf_thetas_low: List[float],
                                      zipf_thetas_high: List[float],
                                      cdf_thetas: Tuple[float],
                                      tick_thetas_low=None,
                                      tick_thetas_high=None,
                                      tail_metric='p999'):
    if not tick_thetas_low:
        tick_thetas_low = zipf_thetas_low
    if not tick_thetas_high:
        tick_thetas_high = zipf_thetas_high
    fig, ((ax_tp_low, ax_tail_low, ax_lat_l),
          (ax_tp_high, ax_tail_high, ax_lat_r)) = get_subplots(nrows=2,
                                                               ncols=3)

    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]

    for ax_tp, ax_tail, zipf_thetas, tick_thetas in \
        [(ax_tp_low, ax_tail_low, zipf_thetas_low, tick_thetas_low),
         (ax_tp_high, ax_tail_high, zipf_thetas_high, tick_thetas_high)]:
        # plot throughput
        tp_df = load_throughput(exper)
        make_subplot(ax=ax_tp, df=tp_df, x_col='zipf_theta', y_col='throughput',
                     z_col='cc_alg', x_range=zipf_thetas, z_range=cc_algs,
                     filters={"thread_cnt": 64})

        # plot tail latency
        tail_df = load_tail(exper)
        make_subplot(ax=ax_tail, df=tail_df, x_col='zipf_theta', y_col=tail_metric,
                     z_col='cc_alg', x_range=zipf_thetas, z_range=cc_algs,
                     filters={"thread_cnt": 64, 'tag': 'all'})

        zipf_ticks = tick_thetas
        ax_tp.set_xticks(zipf_ticks, [f"{t:g}" for t in zipf_ticks])
        ax_tail.set_xticks(zipf_ticks, [f"{t:g}" for t in zipf_ticks])

        ax_tp.set_xlabel('Zipfian theta')
        ax_tail.set_xlabel('Zipfian theta')

    assert len(cdf_thetas) == 2
    for zipf_cdf, ax in zip(cdf_thetas, [ax_lat_l, ax_lat_r]):
        lat_dfs = {cc_alg: load_latency(exper, cc_alg, 64, zipf=zipf_cdf)
                   for cc_alg in cc_algs}
        make_subplot_latency_cdf(ax, lat_dfs, cc_algs)

    return fig, ((ax_tp_low, ax_tail_low, ax_lat_l),
                 (ax_tp_high, ax_tail_high, ax_lat_r))


def plot_tpcc_thread_vs_throughput_tail(exper: str, num_wh=1, tail_metric='p999'):
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        get_subplots(nrows=2, ncols=2)

    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 4, 8, 16, 24, 32, 40, 48, 56, 64]

    # plot throughput
    tp_df = load_throughput(exper)
    make_subplot(ax=ax_tp, df=tp_df, x_col='thread_cnt', y_col='throughput',
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"num_wh": num_wh})

    # plot tail latency
    tail_df = load_tail(exper)
    make_subplot(ax=ax_tail, df=tail_df, x_col='thread_cnt', y_col=tail_metric,
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"num_wh": num_wh, 'tag': 'all'})

    set_x_threads(ax_tp)
    set_x_threads(ax_tail)

    for thd, ax in zip([16, 64], [ax_lat_l, ax_lat_r]):
        lat_dfs = {cc_alg: load_latency(exper, cc_alg, thd, num_wh=num_wh)
                   for cc_alg in cc_algs}
        make_subplot_latency_cdf(ax, lat_dfs, cc_algs)

    return fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r))


def plot_ycsb_prio_ratio_vs_throughput(exper: str):
    fig, ax = plt.subplots(nrows=1, ncols=1)
    set_fig(fig, 1, 1.5)

    pr_range = [0, 0.05, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]

    tp_df = load_throughput(exper)
    make_subplot(ax=ax, df=tp_df, x_col='prio_ratio', y_col='throughput',
                 z_col='cc_alg', x_range=pr_range, z_range=["SILO_PRIO"],
                 filters={"zipf_theta": 0.99})

    ticks = [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9, 1]
    ax.set_xticks(ticks, [f"{t:g}" for t in ticks])
    ax.set_xlabel('High priority transaction ratio')

    return fig, ax


def plot_aria_batch(zipf: float):
    exper = "ycsb_aria_batch"
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        get_subplots(nrows=2, ncols=2)

    cc_algs = ["ARIA_1", "ARIA_2", "ARIA_4", "ARIA_8", "SILO_PRIO"]
    thread_cnts = [1, 4, 8, 16, 24, 32, 40, 48, 56, 64]

    # plot throughput
    tp_df = load_throughput(exper)
    make_subplot(ax=ax_tp, df=tp_df, x_col='thread_cnt', y_col='throughput',
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"zipf_theta": zipf})

    # plot tail latency
    tail_df = load_tail(exper)
    make_subplot(ax=ax_tail, df=tail_df, x_col='thread_cnt', y_col='p999',
                 z_col='cc_alg', x_range=thread_cnts, z_range=cc_algs,
                 filters={"zipf_theta": zipf, 'tag': 'all'})

    set_x_threads(ax_tp)
    set_x_threads(ax_tail)

    set_x_threads(ax_tp)
    set_x_threads(ax_tail)

    for thd, ax in zip([16, 64], [ax_lat_l, ax_lat_r]):
        lat_dfs = {cc_alg: load_latency(exper, cc_alg, thd, zipf=zipf)
                   for cc_alg in cc_algs}
        make_subplot_latency_cdf(ax, lat_dfs, cc_algs)

    return fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r))


def plot_fig1():
    exper = "ycsb_latency"
    thread_cnt = 64
    zipf = 0.99
    fig, (ax_tail, ax_tp) = get_subplots(nrows=1, ncols=2)

    # this is for background, so no silo_prio
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO"]

    lat_dfs = {cc_alg: load_latency(exper, cc_alg, thread_cnt, zipf=zipf)
               for cc_alg in cc_algs}
    make_subplot_latency_cdf(ax_tail, lat_dfs, cc_algs)
    set_lat_ax(ax_tail, 0.5, 6)

    # then draw bar-graph for throughput
    tp_df = load_throughput(exper)
    make_bar(ax_tp, tp_df, cc_algs)
    set_tp_ax(ax_tp, 0.2, 3)
    fig.savefig(f"ycsb_latency_allcc.{IMAGE_TYPE}", transparent=IS_TRANSPARENT)


def plot_fig2():
    fig, ax = plot_ycsb_prio_ratio_vs_throughput("ycsb_prio_sen")
    set_tp_ax(ax, 0.2, 3)
    fig.savefig(f"ycsb_prio_ratio_vs_throughput.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig3():
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        plot_ycsb_thread_vs_throughput_tail("ycsb_thread")
    set_tp_ax(ax_tp, 0.2, 3)
    set_tail_ax(ax_tail, 0.4, 4)
    set_lat_ax(ax_lat_l, 0.2, 4, xlabel_suffix="16 threads")
    set_lat_ax(ax_lat_r, 0.5, 4, xlabel_suffix="64 threads")

    fig.savefig(f"ycsb_thread_vs_throughput_tail.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig4():
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        plot_ycsb_thread_vs_throughput_tail("ycsb_readonly")
    set_tp_ax(ax_tp, 2, 4)
    set_tail_ax(ax_tail, 0.05, 4)
    set_lat_ax(ax_lat_l, 0.01, 4, xlabel_suffix="16 threads")
    set_lat_ax(ax_lat_r, 0.05, 4, xlabel_suffix="64 threads")

    fig.savefig(f"ycsb_thread_vs_throughput_tail_readonly.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig5():
    fig, ((ax_tp_low, ax_tail_low, ax_lat_l),
         (ax_tp_high, ax_tail_high, ax_lat_r)) = plot_ycsb_zipf_vs_throughput_tail(
        "ycsb_zipf", [0, 0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9],
        [0.99, 1.1, 1.2, 1.3, 1.4, 1.5], (0.9, 1.5),
        [0, 0.3, 0.6, 0.9], [0.99, 1.1, 1.2, 1.3, 1.4, 1.5])

    set_tp_ax(ax_tp_low, 1, 4)
    set_tail_ax(ax_tail_low, 0.25, 4)
    set_tp_ax(ax_tp_high, 0.2, 3)
    set_tail_ax(ax_tail_high, 4, 4)
    set_lat_ax(ax_lat_l, 0.4, 3, xlabel_suffix="theta 0.9")
    set_lat_ax(ax_lat_r, 5, 4, xlabel_suffix="theta 1.5")

    # add a zoom-in graph
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    tp_df = load_throughput("ycsb_zipf")

    zipf_ticks_zoom = [1.2, 1.3, 1.4, 1.5]
    ax_tp_zoom = ax_tp_high.inset_axes([0.4, 0.4, 0.55, 0.55])
    make_subplot(ax=ax_tp_zoom, df=tp_df, x_col='zipf_theta', y_col='throughput',
                 z_col='cc_alg', x_range=zipf_ticks_zoom, z_range=cc_algs,
                 filters={"thread_cnt": 64})

    set_tp_ax(ax_tp_zoom, 0.06, 2, ylabel=None)
    ax_tp_zoom.set_xticks(zipf_ticks_zoom, [f"{t:g}" for t in zipf_ticks_zoom])

    fig.savefig(f"ycsb_zipf_vs_throughput_tail.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig6():
    exper = "ycsb_latency"
    thread_cnt = 64
    zipf = 0.99
    fig, (ax_tail, ax_tp) = get_subplots(nrows=1, ncols=2)

    df = load_latency(exper, "SILO", thread_cnt, zipf=zipf)
    make_subplot_latency_cdf(ax_tail, {"SILO": df}, ["SILO"])

    # then SILO_PRIO_FIXED and SILO_PRIO
    for cc_alg in ["SILO_PRIO_FIXED", "SILO_PRIO"]:
        df = load_latency(exper, cc_alg, thread_cnt, zipf=zipf)
        make_cdf(ax_tail, df, f"{cc_alg}:High", [8, 15])
        make_cdf(ax_tail, df, f"{cc_alg}:Low", [0, 7])

    set_lat_ax(ax_tail, 0.5, 6)

    ax_tail.set_xlabel("Latency (ms)")
    ax_tail.set_ylabel("Tail percentage")

    # then draw bar-graph for throughput
    tp_df = load_throughput(exper)
    make_bar(ax_tp, tp_df, ["SILO", "SILO_PRIO_FIXED", "SILO_PRIO"])

    set_tp_ax(ax_tp, 0.2, 3)
    fig.savefig(f"ycsb_latency_udprio.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig7():
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        plot_tpcc_thread_vs_throughput_tail("tpcc_thread", num_wh=1)

    set_tp_ax(ax_tp, 0.1, 3)
    set_tail_ax(ax_tail, 0.4, 4)
    set_lat_ax(ax_lat_l, 0.4, 4, xlabel_suffix="16 threads")
    set_lat_ax(ax_lat_r, 0.4, 4, xlabel_suffix="64 threads")

    fig.savefig(f"tpcc_thread_vs_throughput_tail_wh1.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig8():
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = \
        plot_tpcc_thread_vs_throughput_tail("tpcc_thread", num_wh=64)

    set_tp_ax(ax_tp, 1, 5)
    set_tail_ax(ax_tail, 0.02, 4)
    set_lat_ax(ax_lat_l, 0.02, 4, xlabel_suffix="16 threads")
    set_lat_ax(ax_lat_r, 0.02, 4, xlabel_suffix="64 threads")

    fig.savefig(f"tpcc_thread_vs_throughput_tail_wh64.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig9():
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = plot_aria_batch(zipf=0.5)
    set_tp_ax(ax_tp, 1, 4)
    set_tail_ax(ax_tail, 0.1, 4)
    set_lat_ax(ax_lat_l, 0.05, 4, xlabel_suffix="16 threads")
    set_lat_ax(ax_lat_r, 0.2, 4, xlabel_suffix="64 threads")
    fig.savefig(f"ycsb_aria_thread_vs_throughput_tail_zipf0.5.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def plot_fig10():
    fig, ((ax_tp, ax_tail), (ax_lat_l, ax_lat_r)) = plot_aria_batch(zipf=0.99)
    set_tp_ax(ax_tp, 0.2, 3)
    set_tail_ax(ax_tail, 1, 4)
    set_lat_ax(ax_lat_l, 0.5, 4, xlabel_suffix="16 threads")
    set_lat_ax(ax_lat_r, 2, 4, xlabel_suffix="64 threads")
    fig.savefig(f"ycsb_aria_thread_vs_throughput_tail_zipf0.99.{IMAGE_TYPE}",
                transparent=IS_TRANSPARENT)


def make_legend(keys: List[str],
                fname: str,
                height=0.15,
                ncol=None,
                fontsize=10,
                columnspacing=2):
    if ncol is None:
        ncol = len(keys)
    pseudo_fig = plt.figure()
    ax = pseudo_fig.add_subplot(111)

    lines = []
    for k in keys:
        line, = ax.plot([], [],
                        color=color_map[k],
                        linestyle=linestyle_map[k],
                        marker=marker_map[k],
                        markersize=marker_size,
                        label=label_map[k])
        lines.append(line)

    legend_fig = plt.figure()
    legend_fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    legend_fig.set_size_inches(LEGEND_WIDTH, height)
    legend_fig.legend(lines, [label_map[k] for k in keys],
                      loc='center',
                      ncol=ncol,
                      fontsize=fontsize,
                      frameon=False,
                      columnspacing=columnspacing,
                      labelspacing=0.4)
    legend_fig.savefig(f"{fname}.{IMAGE_TYPE}", transparent=IS_TRANSPARENT)


def make_legend_udprio(height=0.15,
                       columnspacing=1, fontsize=10):
    pseudo_fig = plt.figure()
    ax = pseudo_fig.add_subplot(111)

    cc_algs = ["SILO", "SILO_PRIO_FIXED", "SILO_PRIO"]
    bars = [
        mpatches.Patch(color=color_map[cc], label=label_map[cc])
        for cc in cc_algs
    ]

    line_high, = ax.plot([], [], color='black', linestyle='-', label="High")
    line_low, = ax.plot([], [], color='black', linestyle='--', label="Low")
    lines = [line_high, line_low]

    cc_legend_fig = plt.figure()
    cc_legend_fig.set_size_inches(LEGEND_WIDTH, height)
    cc_legend_fig.legend(lines + bars,
                         ["High", "Low"] + [label_map[cc] for cc in cc_algs],
                         loc='center',
                         ncol=5,
                         fontsize=fontsize,
                         frameon=False,
                         columnspacing=columnspacing,
                         labelspacing=0.4)
    cc_legend_fig.set_tight_layout({"pad": 0, "w_pad": 0, "h_pad": 0})
    cc_legend_fig.savefig(
        f"legend_udprio.{IMAGE_TYPE}", transparent=IS_TRANSPARENT)


if __name__ == "__main__":
    plot_fig1()
    plot_fig2()
    plot_fig3()
    plot_fig4()
    plot_fig5()
    plot_fig6()
    plot_fig7()
    plot_fig8()
    plot_fig9()
    plot_fig10()

    make_legend(["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"],
                "legend_cc", columnspacing=1)
    make_legend(["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO"],
                "legend_4cc", columnspacing=3)
    make_legend(["ARIA_1", "ARIA_2", "ARIA_4", "ARIA_8", "SILO_PRIO"],
                "legend_aria", columnspacing=2)
    make_legend_udprio(columnspacing=2)
