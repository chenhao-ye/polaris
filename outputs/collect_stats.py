import sys, os


f = open(sys.argv[1], 'r')
cc_alg = ''
wl = 'YCSB' 

out_path = "stats.csv"
has_col_name = os.path.exists(out_path)
output = open(out_path, 'a+')

for line in f:
	if "WAIT_DIE" in line:
		cc_alg = "WAIT DIE"
	elif "WOUND_WAIT" in line:
		cc_alg = "WOUND WAIT"
	elif "NO_WAIT" in line:
		cc_alg = "NO WAIT"
	elif "TPCC" in line:
		wl = 'TPCC'
	else:	
		if '[summary]' not in line:
			continue
		# handle summary
		tokens = line.strip().split('summary]')[-1].split(',')
		stat = '{},{},'.format(wl, cc_alg) + ",".join([token.strip().split('=')[1] for token in tokens]) + '\n'
		if not has_col_name:
			output.write('workload,cc_alg,' + ','.join([token.strip().split('=')[0] for token in tokens]) + '\n')
			has_col_name = True
		output.write(stat)
f.close()
output.close()

