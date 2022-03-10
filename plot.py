import matplotlib.pyplot as plt
import pandas as pd


exper_list = ['autoprio_thd']


# based on: https://colorbrewer2.org/#type=qualitative&scheme=Set3&n=5
# this color map ensure the curves are still readable in grayscale
color_map = {
    "WOUND_WAIT": "#80b1d3",
    "NO_WAIT": "#8dd3c7",
    "WAIT_DIE": "#fdaa48",
    "SILO": "#bebada",
    "SILO_PRIO": "#fb8072",
}

linestyle_map = {
    "WOUND_WAIT": "-",
    "NO_WAIT": "-",
    "WAIT_DIE": "-",
    "SILO": "-",
    "SILO_PRIO": "-",
}

marker_map = {
    "WOUND_WAIT": "d",
    "NO_WAIT": "^",
    "WAIT_DIE": "s",
    "SILO": "x",
    "SILO_PRIO": "o", 
}

marker_size = 3

def plot_throughput_vs_thread(exper: str):
    cc_algs = ["WOUND_WAIT", "NO_WAIT", "WAIT_DIE", "SILO", "SILO_PRIO"]
    thread_cnts = [1, 2, 4, 8, 16, 32, 64]
    tp_ticks = list(range(0, 1000001, 200000))
    data_path = f"results/{exper}/throughput.csv"

    fig, ax = plt.subplots(figsize=(5, 2.7), tight_layout='true')
    df = pd.read_csv(data_path, header=0)

    for cc_alg in cc_algs:
        cc_df = df[(df["cc_alg"] == cc_alg)]
        ax.plot(cc_df["thread_cnt"].tolist(),
                cc_df["throughput"].tolist(),
                color=color_map[cc_alg],
                linestyle=linestyle_map[cc_alg],
                marker=marker_map[cc_alg],
                markersize=marker_size,
                label=cc_alg)
    ax.set_xlabel('# threads')
    ax.set_ylabel('Throughput (Mtxn/s)')

    ax.set_xticks(thread_cnts, list(f"{t}" for t in thread_cnts))
    ax.set_yticks(tp_ticks, list(f"{tp/1000000}" if tp > 0 else "0" for tp in tp_ticks), rotation=90)

    ax.legend()

    fig.savefig(f"{exper}-throughput.pdf")

plot_throughput_vs_thread(exper_list[0])
