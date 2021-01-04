import json
from test import *

def eval_arg(job, arg):
    return ((arg in job) and (job[arg] == "true"))

if __name__ == "__main__":
    fname = sys.argv[1]
    print("- read config from file: {}".format(fname))
    job = json.load(open(fname))
    unset_numa = eval_arg(job, "UNSET_NUMA")
    if unset_numa:
        print("- disable numa-aware")
    if eval_arg(job, "COMPILE_ONLY"):
        compile(job)
    else:
        compile_and_run(job, unset_numa=unset_numa)
        if eval_arg(job, "OUTPUT_TO_FILE"):
            stats = open("outputs/stats.json", "a+")
            stats.write(json.dumps(job)+"\n")
            stats.close()
