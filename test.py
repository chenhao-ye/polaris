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

def set_ndebug(ndebug):
    f = open("system/global.h", "a+")
    found = None
    set_ndebug = False
    for line in f:
        if "#define NDEBUG" in line:
            found = line
            if line[0] != '#':
                set_ndebug = True
        if "<cassert" in line:
            break
    if found is None:
        if ndebug:
            replace("system/global.h", r"#include <cassert>", "#define NDEBUG\n#include <cassert>")
    else:
        if ndebug:
            replace("system/global.h", found, "#define NDEBUG\n")
        else:
            replace("system/global.h", found, "")

def compile(job):
        os.system("cp {} {}".format(dbms_cfg[0], dbms_cfg[1]))
	# define workload
        for (param, value) in job.iteritems():
		pattern = r"\#define\s*" + re.escape(param) + r'[\t ].*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
	os.system("make clean > temp.out 2>&1")
	ret = os.system("make -j8 > temp.out 2>&1")
	if ret != 0:
		print "ERROR in compiling, output saved in temp.out"
		exit(0)
	else:
		os.system("rm -f temp.out")

def run(test = '', job=None, numa=True):
	app_flags = ""
	if test == 'read_write':
		app_flags = "-Ar -t1"
	if test == 'conflict':
		app_flags = "-Ac -t4"
        if numa:
	    os.system("numactl --interleave all ./rundb %s | tee temp.out" % app_flags)
        else:
            os.system("./rundb %s | tee temp.out" % app_flags)
	
def eval_arg(job, arg):
    return ((arg in job) and (job[arg] == "true"))

def parse_output(job):
    output = open("temp.out")
    success = False
    for line in output:
            line = line.strip()
            if "[summary]" in line:
                    success = True
                    for token in line.strip().split('[summary]')[-1].split(','):
                            key, val = token.strip().split('=')
                            job[key] = val
                    break
    if success:
        output.close()
        os.system("rm -f temp.out")
        return job
    errlog = open("log/{}.log".format(datetime.datetime.now().strftime("%b-%d_%H-%M-%S-%f")), 'a+')
    errlog.write("{}\n".format(json.dumps(job)))
    output = open("temp.out")
    for line in output:
        errlog.write(line)
    errlog.close()
    output.close()
    os.system("rm -f temp.out")
    return job

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
            key, value = item.split("=", 1)
            job[key] = value
    if not eval_arg(job, "EXEC_ONLY"):
        print("- compiling...")
        ndebug = eval_arg(job, "NDEBUG")
        set_ndebug(ndebug)
        if ndebug:
            print("- disable assert()")
        compile(job)
    numa = eval_arg(job, "UNSET_NUMA") == False
    if not numa:
        print("- disable interleaving allocation across numa nodes")
    if not eval_arg(job, "COMPILE_ONLY"):
        print("- executing...")
        run("", job, numa=numa)
        if eval_arg(job, "OUTPUT_TO_FILE"):
            job = parse_output(job)
            stats = open("outputs/stats.json", "a+")
            stats.write(json.dumps(job)+"\n")
            stats.close()

