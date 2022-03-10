import matplotlib.pyplot as plt
import pandas as pd
import numpy as np

# based on: https://colorbrewer2.org/#type=qualitative&scheme=Set3&n=5
# this color map ensure the curves are still readable in grayscale
color_map = {
    "NO_WAIT": "#8dd3c7",
    "WAIT_DIE": "#80b1d3",
    "WOUND_WAIT": "#bebada",
    "SILO": "#fdaa48",
    "SILO_PRIO": "#fb8072",
}

# linestyle and marker are unused if use bar instead of plot
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


def plot_throughput_vs_thread(exper: str):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 2, 4, 8, 16, 32, 64]
    tp_ticks = list(range(0, 1000001, 200000))
    data_path = f"results/{exper}/throughput.csv"

    x = np.arange(len(thread_cnts))
    width = 0.15

    fig, ax = plt.subplots(figsize=(5, 2.7))
    df = pd.read_csv(data_path, header=0)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg)]
        ax.bar(x=x + width * (i + 1.5 - len(thread_cnts) / 2),
               height=cc_df["throughput"].tolist(),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('# threads')
    ax.set_ylabel('throughput (Mtxn/s)')

    ax.set_xticks(x, list(f"{t}" for t in thread_cnts))
    ax.set_yticks(tp_ticks, list(
        f"{tp/1000000}" if tp > 0 else "0" for tp in tp_ticks), rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-throughput.pdf")


def plot_throughput_vs_zipf(exper: str, thread_cnt: int = None):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    zipf_theta_list = [0.1, 0.3, 0.5, 0.7, 0.9, 0.99, 1.1, 1.3, 1.5]
    tp_ticks = list(range(0, 1000001, 200000))
    data_path = f"results/{exper}/throughput.csv"

    x = np.arange(len(zipf_theta_list))
    width = 0.15

    fig, ax = plt.subplots(figsize=(5, 2.7))
    df = pd.read_csv(data_path, header=0)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg)]
        if thread_cnt is not None:
            cc_df = cc_df[(cc_df["thread_cnt"] == thread_cnt)]
        ax.bar(x=x + width * (i + 2.5 - len(zipf_theta_list) / 2),
               height=cc_df["throughput"].tolist(),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('zipf_theta')
    ax.set_ylabel('throughput (Mtxn/s)')

    ax.set_xticks(x, list(f"{t}" for t in zipf_theta_list))
    ax.set_yticks(tp_ticks, list(
        f"{tp/1000000}" if tp > 0 else "0" for tp in tp_ticks), rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-{thread_cnt}-throughput.pdf")


def plot_tail_latency_vs_thread(exper: str, metric: str = "p999"):
    cc_algs = ["NO_WAIT", "WAIT_DIE", "WOUND_WAIT", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 2, 4, 8, 16, 32, 64]
    latency_ticks = list(range(0, 401, 100))
    data_path = f"results/{exper}/tail.csv"

    x = np.arange(len(thread_cnts))
    width = 0.15

    fig, ax = plt.subplots(figsize=(5, 2.7))
    df = pd.read_csv(data_path, header=0)

    for i, cc_alg in enumerate(cc_algs):
        cc_df = df[(df["cc_alg"] == cc_alg)]
        cc_df = cc_df[(cc_df["tag"] == "all")]
        print(list(float(l) for l in cc_df[metric].tolist()))
        ax.bar(x=x + width * (i + 1.5 - len(thread_cnts) / 2),
               height=list(float(l) for l in cc_df[metric].tolist()),
               width=width,
               color=color_map[cc_alg],
               label=cc_alg)
    ax.set_xlabel('# threads')
    ax.set_ylabel(f'{metric} tail latency (us)')

    ax.set_xticks(x, list(f"{t}" for t in thread_cnts))
    ax.set_yticks(latency_ticks, list(
        f"{l}" for l in latency_ticks), rotation=90)

    ax.legend()
    fig.tight_layout()
    fig.savefig(f"{exper}-tail-{metric}.pdf")


plot_throughput_vs_thread("autoprio_thd")
plot_throughput_vs_zipf("autoprio_zipf", 32)
# plot_throughput_vs_zipf("autoprio_zipf", 64)
plot_tail_latency_vs_thread("autoprio_thd", "p999")
