#!/usr/bin/python2.7  
# -*- coding: utf-8 -*- 

# 数据 输入与预处理相关功能

import pandas as pd
import numpy as np 
from utils import Job, QueueConfig


# CAPACITY_INDEX = 1 
# MAX_CAPACITY_INDEX = 3
# ABS_CAPACITY = 4
# QUEUE_NAME_INDEX = 8
# QUEUE_STATE_INDEX = 9
def read_scheduler_csv(path):
    confs = [] 
    queue = ['spark', 'hive']
    for i in range(2):
        queue_config = QueueConfig()
        queue_config.capacity = 50.0
        queue_config.max_capacity = 50.0
        queue_config.abs_capacity = 50.0
        queue_config.name = queue[i] 
        queue_config.state = 'RUNNING'
        confs.append(queue_config)
        """
        queue_config.display()
        """
    return confs


# TOTOL_MB_INDEX = 15
def read_cluster_csv(path):
    total_mb = 16 * 1024
    return total_mb

# QUEUE_INDEX = 3
# START_TIME_INDEX = 14
# FINISHED_TIME_INDEX = 15
# RUN_TIME_INDEX = 16
# MEMORY_SECONDS_INDEX = 22
def read_app_csv(path):
    job_count = np.random.randint(10, 20)
    queue = ['spark', 'hive']
    jobs = []
    for i in range(job_count):
        job = Job()
        job.name = queue[np.random.randint(0,2)] 
        job.wait_time = np.random.randint(0, 25)
        job.run_time = np.random.randint(10, 40)
        job.memory_seconds = 1024*job.run_time
        jobs.append(job)
        """
        print '%d: queue: %s, wait time: %d, run time: %d, memory seconds: %d' %(i, job.name, job.wait_time, job.run_time, job.memory_seconds)
        """
    return jobs

if __name__ == '__main__':
    jobs = read_app_csv('./hadoop/app.csv')
    """
    """
    # read_scheduler_csv('./hadoop/scheduler2.csv')
    # print read_cluster_csv('./hadoop/cluster2.csv')
