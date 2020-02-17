import os
import sys


check_dir = sys.argv[1]


#print 'alg threads txn_len num_hs pos_hs synthetic zipfian read_ratio throughput debug2 debug9 debug10 abort_cnt txn_cnt'
print 'alg threads txn_len num_hs pos_hs synthetic zipfian read_ratio specified throughput debug2 debug9 debug10 abort_cnt txn_cnt'
#print 'alg threads txn_len num_hs pos_hs synthetic throughput debug2 debug9 debug10 abort_cnt txn_cnt'

results = []


for subdir in os.listdir(check_dir):
    #print subdir
    res = ''
    res1 = ''
    res2 = ''
    res3 = ''
    jump = False
    #print subdir
    for each_file in os.listdir(check_dir + '/' + subdir):
       if each_file == 'config.json':
           with open(check_dir + '/' + subdir + '/' + each_file, 'r')  as config_file:
               dict_from_file = eval(config_file.read())
               #res1 = dict_from_file['alg'] + ' ' + str(dict_from_file['threads']) + ' ' + str(dict_from_file['txn_len']) + ' ' + str(dict_from_file['num_hs']) + ' ' + dict_from_file['pos_hs'] + ' ' +  str(dict_from_file['synthetic']) + ' ' + str(dict_from_file['zipfian']) + ' ' + str(dict_from_file['read_ratio']) + ' '
               res1 = dict_from_file['alg'] + ' ' + str(dict_from_file['threads']) + ' ' + str(dict_from_file['txn_len']) + ' ' + str(dict_from_file['num_hs']) + ' ' + dict_from_file['pos_hs'] + ' ' +  str(dict_from_file['synthetic']) + ' ' + str(dict_from_file['zipfian']) + ' ' + str(dict_from_file['read_ratio']) + ' ' + str(dict_from_file['specified']) + ' '
               #res1 = dict_from_file['alg'] + ' ' + str(dict_from_file['threads']) + ' ' + str(dict_from_file['txn_len']) + ' ' + str(dict_from_file['num_hs']) + ' ' + dict_from_file['pos_hs'] + ' ' +  str(dict_from_file['synthetic']) + ' ' 
               if (dict_from_file['alg'] != 'CLV'):
                   jump = False
       if each_file == 'stats.json':
           with open(check_dir + '/' + subdir + '/' + each_file, 'r')  as output_file:
               dict_from_file = eval(output_file.read())
               #res2 = dict_from_file['throughput'] + ' '
               res2 = dict_from_file['throughput'] + ' ' + dict_from_file['debug2'] + ' ' + dict_from_file['debug9'] + ' ' + dict_from_file['debug10'] + ' ' + dict_from_file['abort_cnt'] + ' ' + dict_from_file['txn_cnt'] + ' '

               #lines = fio_output_file.readlines()
               #res2 = lines[-1].split('sum:')[-1].strip('\n')

    if jump == True:
        continue
    print res1+res2+res3, subdir
    #results.append(res1+res2+res3)

#print("\n".join(sorted(results)))
