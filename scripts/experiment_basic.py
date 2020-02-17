import itertools
from pyreuse.helpers import *
from pyreuse.sysutils.cgroup import Cgroup
import os.path
import time
import json
import os
import datetime

from pyreuse.sysutils.blocktrace import *
from pyreuse.sysutils.ncq import *
from pyreuse.sysutils.iostat_parser import *

# basis
KB = 1024
MB = 1024 * KB
GB = 1024 * MB 


# experiment environment



# experiment setup


class Experiment(object):
    def __init__(self):
        # config something
        #self.exp_name = 'clv_onehotspot_50000penalty'
        #self.exp_name = 'no_hotspot'
        self.exp_name = 'zipfian_ww_starvation'
        self.home_dir = '/users/kanwu/'
        self.res_dir = self.home_dir + 'results/' + self.exp_name
        self.tmp_dir = '/dev/shm/'
        prepare_dir(self.res_dir)
       
        # tools config
        self.tools_config = {
            'clear_page_cache': False,   # whether clear page cache before each run 
            'blktrace'        : False,   # check block IOs
            'iostat'          : False,  # check ios and cpu/io utilization
            'perf'            : False,  # draw flamegraph
            'sar'             : False   # check page faults
        }

        # experiment config
        config = {
          'alg': ['WOUND_WAIT'],     #'mmap' 'libaio'
          #'alg': ['NO_WAIT', 'CLV', 'SILO', 'WOUND_WAIT', 'WAIT_DIE'],     #CLV SILO WOUND_WAIT WAIT_DIE
          #'alg': ['NO_WAIT', 'CLV', 'SILO', 'WAIT_DIE'],     #CLV SILO WOUND_WAIT WAIT_DIE
          #'threads': [1, 2, 4, 8, 16, 32, 64],
          'threads': [1, 2, 4, 8, 16, 32],
          #'threads': [16],
          #'txn_len': [4, 16, 64],    # number of requests in the txn
          'txn_len': [16],    # number of requests in the txn
          #'num_hs' : [1], # 0, 1, 2
          'num_hs' : [0],
          #'pos_hs' : ['TOP', 'MID', 'BOT'], # this is only for one hotspot
          'pos_hs' : ['TOP'],
          #'pos_hs' : ['TM', "MB"],
          'synthetic': ['false'],  # true is for synthetic workloads
          #'zipfian' : [0, 0.1, 0.3, 0.5, 0.7, 0.9],   
          'zipfian' : [0, 0.7, 0.9, 0.99],   
          #'read_ratio' : [0, 0.1, 0.3, 0.5, 0.7, 0.9, 1.0],
          #'read_ratio' : [0, 0.5, 0.9],
          'read_ratio' : [0, 0.5, 0.9],
        }

        # handle
        self.handle_config(config) 
        print '========================= overall ', len(self.all_configs), ' experiments ============================='
        print '==================== results in ', self.res_dir, ' ============================'
 
    def handle_config(self, config):
        config_dic = list(config.keys())
        config_lists = list(config.values())

        self.all_configs = []
        for element in itertools.product(*config_lists):
            new_config = dict(list(itertools.izip(config_dic, list(element))))
            self.all_configs.append(new_config)
        
    def dump_config(self, config):
        self.cur_exp_dir = self.res_dir + '/' + datetime.now().strftime("%H-%M-%S_%Y%m%d")
        os.mkdir(self.cur_exp_dir)
        with open(self.cur_exp_dir + '/config.json', 'w') as config_output:
            json.dump(config, config_output)

    def before_each(self, config):
        print '                ********* Configured with **********'
        print config
        self.dump_config(config)
        #TODO create job file
        with open('/users/kanwu/DBx1000-Private/scripts/synthetic_debug.sh.template', 'r') as input_file, open('/users/kanwu/DBx1000-Private/synthetic_debug.sh', 'w') as output_file:
            for line in input_file:
                if 'alg=' in line:
                    output_file.write('alg=' + str(config['alg']) + '\n')
                    continue
                if 'threads=' in line:
                    output_file.write('threads=' + str(config['threads']) + '\n')
                    continue
                if 'req=' in line:
                    output_file.write('req=' + str(config['txn_len']) + '\n')
                    continue
                if 'num_hs=' in line:
                    output_file.write('num_hs=' + str(config['num_hs']) + '\n')
                    continue
                if 'pos=' in line:
                    output_file.write('pos=' + str(config['pos_hs']) + '\n')
                    continue
                if 'synthetic=' in line:
                    output_file.write('synthetic=' + str(config['synthetic']) + '\n')
                    continue
                if 'zipf=' in line:
                    output_file.write('zipf=' + str(config['zipfian']) + '\n')
                    continue
                if 'read_ratio=' in line:
                    output_file.write('read_ratio=' + str(config['read_ratio']) + '\n')
                    continue
                output_file.write(line)

        # clear page cache
        if self.tools_config['clear_page_cache']:
            shcmd('sync; echo 3 > /proc/sys/vm/drop_caches; sleep 5')
        # start iostat
        if self.tools_config['iostat']:
            shcmd('pkill iostat', ignore_error = True)
            shcmd('iostat -mx 1 /dev/nvme0n1p4 > ' + self.tmp_dir + '/iostat.out &')
            #shcmd('iostat -mx 1 /dev/nvme0n1p3 > ' + self.cur_exp_dir + '/iostat.out &')   # to identify swapping

        # start blktrace
        stop_blktrace_on_bg()
        if self.tools_config['blktrace']:
            start_blktrace_on_bg('/dev/nvme0n1p4', self.tmp_dir +'/blktrace.output', ['issue', 'complete'])
       
        # start perf
        shcmd('pkill perf', ignore_error = True)
        if self.tools_config['perf']:
            shcmd('rm perf.data', ignore_error = True)
            shcmd('perf record -F 99 -a -g -- sleep 30 &')
            #shcmd('perf record -F 99 -a -g -- sleep 60 &')

        # kill exp workload
        #shcmd('pkill -9 fio', ignore_error = True)
   
        # start sar to trace page faults
        shcmd('pkill sar', ignore_error = True)
        if self.tools_config['sar']:
            shcmd('sar -B 1 > ' + self.tmp_dir + '/sar.out &')


 
    def exp(self, config):
        print '              *********** start running ***********'
        shcmd('rm /users/kanwu/DBx1000-Private/outputs/stats.json', ignore_error = True)
        cmd = 'bash /users/kanwu/DBx1000-Private/synthetic_debug.sh > /dev/shm/running'
        print cmd
        shcmd(cmd)
    
    def handle_iostat_out(self, iostat_output):
        print "==== utilization statistics ===="
        stats = parse_batch(iostat_output.read())
        with open(self.cur_exp_dir + '/iostat.out.cpu_parsed', 'w') as parsed_iostat:
            parsed_iostat.write('iowait system user idle \n')
            item_len = average_iowait = average_system = average_user = average_idle = 0
            for item in stats['cpu']:
                parsed_iostat.write(str(item['iowait']) + ' ' + str(item['system']) + ' ' + str(item['user']) + ' ' + str(item['idle']) + '\n')
                if float(item['idle']) > 79:
                    continue
                item_len += 1
                average_iowait += float(item['iowait'])
                average_system += float(item['system'])
                average_user += float(item['user'])
                average_idle += float(item['idle'])
            if item_len > 0:
                print 'iowait  system  user  idle'
                print str(average_iowait/item_len), str(average_system/item_len), str(average_user/item_len), str(average_idle/item_len)
            else:
                print 'seems too idle of CPU'

        with open(self.cur_exp_dir + '/iostat.out.disk_parsed', 'w') as parsed_iostat:
            parsed_iostat.write('r_iops r_bw(MB/s) w_iops w_bw(MB/s) avgrq_sz(KB) avgqu_sz\n')
            item_len = average_rbw = average_wbw = 0
            for item in stats['io']:
                parsed_iostat.write(item['r/s'] + ' ' + item['rMB/s'] + ' ' + item['w/s'] + ' ' + item['wMB/s'] + ' ' + str(float(item['avgrq-sz'])*512/1024) + ' '+ item['avgqu-sz'] +'\n')
                if float(item['rMB/s']) + float(item['wMB/s']) < 20:
                    continue
                item_len += 1
                average_rbw += float(item['rMB/s'])
                average_wbw += float(item['wMB/s'])
            if item_len > 0:
                print str(average_rbw/item_len), str(average_wbw/item_len)
            else:
                print 'seems too idle of Disk'
        print "================================="    

    def after_each(self, config):
        time.sleep(5)
        print '              **************** done ***************'

        shcmd('cp /dev/shm/running ' + self.cur_exp_dir + '/')
        shcmd('cp /users/kanwu/DBx1000-Private/outputs/stats.json ' + self.cur_exp_dir + '/' ,ignore_error= True)
        shcmd('cp /users/kanwu/DBx1000-Private/synthetic_debug.sh ' + self.cur_exp_dir + '/')
       
        # wrapup iostat
        if self.tools_config['iostat']:
            shcmd('pkill iostat; sleep 2')
            with open(self.tmp_dir + '/iostat.out') as iostat_output:
                self.handle_iostat_out(iostat_output)
            shcmd('cp ' + self.tmp_dir + '/iostat.out ' + self.cur_exp_dir + '/iostat.out')
        # wrapup sar
        if self.tools_config['sar']:
            shcmd('pkill sar')
            shcmd('cp ' + self.tmp_dir + '/sar.out ' + self.cur_exp_dir + '/sar.out')

        # wrapup flamegraph(perf)
        if self.tools_config['perf']:
            shcmd('cp perf.data ' + self.cur_exp_dir + '/')
            #shcmd('perf script | /mnt/ssd/fio_test/experiments/FlameGraph/stackcollapse-perf.pl > ' + self.cur_exp_dir + '/out.perf-folded')
            #shcmd('perf script | /mnt/ssd/fio_test/experiments/FlameGraph/stackcollapse-perf.pl > out.perf-folded')
            #shcmd('/mnt/ssd/fio_test/experiments/FlameGraph/flamegraph.pl out.perf-folded > ' + self.cur_exp_dir + '/perf-kernel.svg')
        
        # wrapup blktrace
        if self.tools_config['blktrace']:
            stop_blktrace_on_bg()
            blkresult = BlktraceResultInMem(
                    sector_size=512,
                    event_file_column_names=['pid', 'action', 'operation', 'offset', 'size',
                        'timestamp', 'pre_wait_time', 'sync'],
                    raw_blkparse_file_path=self.tmp_dir+'/blktrace.output',
                    parsed_output_path=self.cur_exp_dir+'/blkparse-output.txt.parsed')
            shcmd('cp ' + self.tmp_dir + '/blktrace.output ' + self.cur_exp_dir + '/blktrace.output')
            
            blkresult.create_event_file()
        
            # generate ncq
            table = parse_ncq(event_path = self.cur_exp_dir + '/blkparse-output.txt.parsed')
            with open(self.cur_exp_dir + '/ncq.txt','w') as ncq_output:
                for item in table:
                    ncq_output.write("%s\n" % ' '.join(str(e) for e in [item['pid'], item['action'], item['operation'], item['offset'], item['size'], item['timestamp'], item['pre_depth'], item['post_depth']]))

    def run(self):
        for config in self.all_configs:
            self.before_each(config)
            self.exp(config)
            self.after_each(config)

if __name__=='__main__':

    exp = Experiment()
    exp.run()
