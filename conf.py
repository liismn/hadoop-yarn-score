
# -*- coding: utf-8 -*- 
import json
import os
import time
from utils import Singleton

class Config(object):
    __metaclass__ = Singleton
    def __init__(self, path=None):
        cur_time = time.time()
        self.config_file_path = path # fixed
        self.scheduler_metric_path = '../hadoop_util/output/scheduler2.csv' # to be fixed
        self.job_metric_path = '../hadoop_util/output/app.csv' # to be fixed
        self.cluster_metric_path = '../hadoop_util/output/cluster2.csv'
        # self.yarn_config_path = './my_test/yarn_config.xml'
        self.yarn_config_path = './conf/yarn_config.xml'
        self.prediction_path = './hadoop/prediction.csv'
        self.stat_output_file = './hadoop/stat.txt'
        self.stat_interval = 2 *60 * 60 * 1000 # 2 hours
        self.total_sys_memory = 65536
        self.rest_port = 5000

        """
        self.update_interval = 60 * 3 * 1000 # three minutes
        self.next_update_time = cur_time 
        self.scheduler_metric_ts = cur_time 
        self.job_metric_ts = cur_time
        self.cluster_metric_ts = cur_time
        self.next_stat_time = cur_time
        self.yarn_config_ts = cur_time
        """

    def get_cluster_metric_path(self):
        return self.cluster_metric_path

    def set_cluster_metric_path(self, path):
        self.cluster_metric_path = path

    def get_prediction_path(self):
        return self.prediction_path

    def set_prediction_path(self, path):
        self.prediction_path = path

    def get_stat_output_file(self):
        return self.stat_output_file

    def set_stat_output_file(self, path):
        self.stat_output_file = path

    def get_scheduler_metric_path(self):
        return self.scheduler_metric_path

    def set_scheduler_metric_path(self, path):
        self.scheduler_metric_path = path

    def get_job_metric_path(self):
        return self.job_metric_path

    def set_job_metric_path(self, path):
        self.job_metric_path = path

    def get_stat_interval(self):
        return self.stat_interval

    def set_stat_interval(self, interval):
        self.stat_interval = interval

    def get_sys_total_memory(self):
        return self.total_sys_memory

    def set_sys_total_memory(self, size):
        self.total_sys_memory = size

    def get_rest_port(self):
        return self.rest_port
   
    def set_rest_port(self, port):
        self.rest_port = port

    def get_yarn_config_path(self):
        return self.yarn_config_path

    def set_yarn_config_path(self, path):
        self.yarn_config_path = path

    """
    def get_update_interval(self):
        return self.update_interval
   
    def set_update_interval(self, interval):
        self.update_interval = interval
 
    def update_config_timestamp(self, ts):
        self.yarn_config_ts = ts

    def get_config_timestamp(self):
        return self.yarn_config_ts

    def update_scheduler_timestamp(self, ts):
        self.scheduler_metric_ts = ts

    def get_scheduler_timestamp(self):
        return self.scheduler_metric_ts

    def update_cluster_timestamp(self, ts):
        self.cluster_metric_ts = ts

    def get_cluster_timestamp(self):
        return self.cluster_metric_ts

    def update_job_timestamp(self, ts):
        self.job_metric_ts = ts

    def get_job_timestamp(self):
        return self.job_metric_ts

    def update_next_stat_time(self):
        self.next_stat_time += self.stat_interval

    def get_next_stat_time(self):
        return self.next_stat_time

    def update_next_update_time(self):
        self.next_update_time += self.update_interval

    def get_next_update_time(self):
        return self.next_update_time 

    def init_all_timestamp(self):
        now = time.time()
        self.next_update_time = now
        self.next_stat_time = now
        self.update_next_stat_time()
        
    """
    def update_config(self):
        with open(self.config_file_path) as f:
            data = json.load(f)

        self.set_job_metric_path(data['job_metric_path'])
        self.set_scheduler_metric_path(data['scheduler_metric_path'])
        self.set_stat_interval(data['stat_interval'])
        self.set_prediction_path(data['prediction_path'])
        # self.set_update_interval(data['update_interval'])
        self.set_cluster_metric_path(data['cluster_metric_path'])
        self.set_stat_output_file(data['stat_output_file'])
        self.set_sys_total_memory(data['sys_total_memory'])
        self.set_yarn_config_path(data['yarn_config_file'])
        self.set_rest_port(data['rest_port'])

    def display(self):
        print('--------------------------------')
        print('job_metric_path: \t%s' % self.job_metric_path)
        print('scheduler_metric_path: \t%s' % self.scheduler_metric_path)
        print('cluster_metric_path: \t%s' % self.cluster_metric_path)
        print('prediction_path \t%s' % self.prediction_path)
        # print('update_interval: \t%ld' % self.update_interval)
        print('stat_interval: \t%ld' % self.stat_interval)
        print('stat_output_file \t%s' % self.stat_output_file)
        print('sys_total_memory \t%ld' % self.total_sys_memory)
        print('yarn_config_path \t%s' % self.yarn_config_path)
        print('rest_port \t%d' % self.rest_port)
        print('--------------------------------')

def get_mtime(filename):
    info = os.stat(filename)
    return info.st_mtime

if __name__ == '__main__':
    conf = Config('./conf/config.json')
    conf.display()
    conf.update_config()
    conf.display()
    print(get_mtime(conf.yarn_config_path))
