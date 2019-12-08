import sys, os


f = open(sys.argv[1], 'r')
cc_alg = ''

out_path = "stats.csv"
has_col_name = os.path.exists(out_path)
output = open(out_path, 'a+')

for line in f:
	if "[CONFIGURATION]" in line:
		config = ",".join(line.strip().split()[1:])
	elif "WAIT_DIE" in line:
		cc_alg = "WAIT DIE"
	elif "WOUND_WAIT" in line:
		cc_alg = "WOUND WAIT"
	elif "NO_WAIT" in line:
		cc_alg = "NO WAIT"
	else:	
		if '[summary]' not in line:
			continue
		# handle summary
		tokens = line.strip().split('summary]')[-1].split(',')
		stat = "{},{},{}\n".format(config, cc_alg, ",".join([token.strip().split('=')[1] for token in tokens]))
		if not has_col_name:
			output.write('workload,threads,max_txn_per_part,abort_penalty,cc_alg,' + ','.join([token.strip().split('=')[0] for token in tokens]) + '\n')
			has_col_name = True
		output.write(stat)
f.close()
output.close()

