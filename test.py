import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal, json


jobs = {}
dbms_cfg = ["config-std.h", "config.h"]
algs = []

def replace(filename, pattern, replacement):
	f = open(filename)
	s = f.read()
	f.close()
	s = re.sub(pattern,replacement,s)
	f = open(filename,'w')
	f.write(s)
	f.close()


def compile(job):
        os.system("cp {} {}".format(dbms_cfg[0], dbms_cfg[1]))
	# define workload
        for (param, value) in job.iteritems():
		pattern = r"\#define\s*" + re.escape(param) + r'.*'
		replacement = "#define " + param + ' ' + str(value)
		replace(dbms_cfg[1], pattern, replacement)
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
	

def compile_and_run(job, unset_numa=False) :
	compile(job)
	run('', job, unset_numa=unset_numa)

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
	# job = {
	# 	"WORKLOAD"			: workload,
	# 	"CC_ALG"			: alg,
	# 	"ABORT_PENALTY"			: penalty,
	# 	"THREAD_CNT"			: threads,
	# 	"MAX_TXN_PER_PART"		: max_txn_cnt,		
	# 	"NUM_WH"			: warehouse
	# }

	job = {}
	for item in sys.argv[1:]:
		key = item.split("=")[0]
		value = item.split("=")[1]
		job[key] = value

        if ("UNSET_NUMA" in job) and job["UNSET_NUMA"]:
            compile_and_run(job, unset_numa=True)
        else:
            compile_and_run(job)
	job = parse_output(job)
	stats = open("outputs/stats.json", 'a+')
	stats.write(json.dumps(job)+"\n")
	stats.close()
	


