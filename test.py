import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal, json


dbms_cfg = ["config-std.h", "config.h"]

def replace(filename, pattern, replacement):
	f = open(filename)
	s = f.read()
	f.close()
	s = re.sub(pattern,replacement,s)
	f = open(filename,'w')
	f.write(s)
	f.close()

def compile(job, ndebug=False):
        os.system("cp {} {}".format(dbms_cfg[0], dbms_cfg[1]))
	# define workload
        for (param, value) in job.iteritems():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
        if ndebug:
            f = open(dbms_cfg[1], "a+")
            f.write("\n#define NDEBUG\n")
            f.close()
	os.system("make clean > temp.out 2>&1")
	ret = os.system("make -j8 > temp.out 2>&1")
	if ret != 0:
		print "ERROR in compiling, output saved in temp.out"
		exit(0)
	else:
		os.system("rm -f temp.out")

def run(test = '', job=None, unset_numa=False):
	app_flags = ""
	if test == 'read_write':
		app_flags = "-Ar -t1"
	if test == 'conflict':
		app_flags = "-Ac -t4"
        if unset_numa:
	    os.system("numactl --all ./rundb %s | tee temp.out" % app_flags)
        else:
            os.system("./rundb %s | tee temp.out" % app_flags)
	
def eval_arg(job, arg):
    return ((arg in job) and (job[arg] == "true"))

if __name__ == "__main__":
    print("usage: path/to/json [more args]")
    fname = sys.argv[1]
    idx = 2
    if ".json" not in fname:
        fname = "experiments/default.json"
        idx = 1
    print("- read config from file: {}".format(fname))
    job = json.load(open(fname))
    if len(sys.argv) > idx:
        # has more args / overwrite existing args
        for item in sys.argv[idx:]:
            key = item.split("=")[0]
            value = item.split("=")[1]
            job[key] = value
    ndebug = eval_arg(job, "NDEBUG"):
    unset_numa = eval_arg(job, "UNSET_NUMA")
    if unset_numa:
        print("- disable numa-aware")
    compile(job, ndebug=ndebug)
    if not eval_arg(job, "COMPILE_ONLY"):
        run("", job, unset_numa=unset_numa)
        if eval_arg(job, "OUTPUT_TO_FILE"):
            stats = open("outputs/stats.json", "a+")
            stats.write(json.dumps(job)+"\n")
            stats.close()
