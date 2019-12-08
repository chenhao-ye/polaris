import os, sys, re, os.path
import platform
import subprocess, datetime, time, signal


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


def run(test = '', job=None):
	app_flags = ""
	if test == 'read_write':
		app_flags = "-Ar -t1"
	if test == 'conflict':
		app_flags = "-Ac -t4"
	os.system("./rundb %s" % app_flags)
	

def compile_and_run(job) :
	compile(job)
	run('', job)

if __name__ == "__main__":
	# TODO: use argparse to set params. default settings 
	#penalty = 100000
	#max_txn_cnt = 1000000
	#threads = 32
	workload = "TPCC"

	# parse workload
	if sys.argv[1].lower() != workload.lower():
		workload = "YCSB"
	
	# parse algorithms
	alg = sys.argv[2]

	# parse threads
	threads = int(sys.argv[3])

	# parse max txn cnt
	max_txn_cnt = int(sys.argv[4])

	# parse penalty
	penalty = int(sys.argv[5])

	# parse warehouse
	warehouse = int(sys.argv[6])

	job = {
		"WORKLOAD"			: workload,
		"CC_ALG"			: alg,
		"ABORT_PENALTY"			: penalty,
		"THREAD_CNT"			: threads,
		"MAX_TXN_PER_PART"		: max_txn_cnt,		
		"NUM_WH"			: warehouse
	}
	print("[CONFIGURATION] {} {} {} {} {} {}".format(workload, threads, max_txn_cnt, penalty, alg, warehouse))	
	compile_and_run(job)
	


