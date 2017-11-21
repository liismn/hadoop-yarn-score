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
    obj = pd.read_csv(path)
    cols = obj.columns.tolist()
    confs = [] 
    for row in range(obj.shape[0]):
        queue_config = QueueConfig()
        queue_config.capacity = obj.iloc[row][1]
        queue_config.max_capacity = obj.iloc[row][3]
        queue_config.abs_capacity = float(obj.iloc[row][4])
        queue_config.name = obj.iloc[row][8]
        queue_config.state = obj.iloc[row][9]
        confs.append(queue_config)
        """
        for i in range(obj.shape[1]):
            print(i, cols[i], obj.iloc[row][i])
        print('-----------------------------------')
        """
    return confs


# TOTOL_MB_INDEX = 15
def read_cluster_csv(path):
    obj = pd.read_csv(path)
    #cols = obj.columns.tolist()
    total_mb = 0
    total_mb = obj.iloc[0][15] # csv should contain only one data line 
    return total_mb

# QUEUE_INDEX = 3
# START_TIME_INDEX = 14
# FINISHED_TIME_INDEX = 15
# RUN_TIME_INDEX = 16
# MEMORY_SECONDS_INDEX = 22
def read_app_csv(path):
    obj = pd.read_csv(path)
    cols = obj.columns.tolist()
    jobs = []
    for row in range(obj.shape[0]):
        job = Job()
        job.name = obj.iloc[row][3]
        job.wait_time = np.random.randint(50) #暂时用随机数模拟
        job.run_time = obj.iloc[row][16] * 0.001 
        job.memory_seconds = obj.iloc[row][22]
        jobs.append(job)
        """
        for i in range(obj.shape[1]):
            print(i, cols[i], obj.iloc[row][i])
        print( '-----------------------------------')
        """
    return jobs

if __name__ == '__main__':
    jobs = read_app_csv('./hadoop/app.csv')
    for i in range(len(jobs)):
        job = jobs[i]
        print(i)
        job.display()
    """
    """
    read_scheduler_csv('./hadoop/scheduler2.csv')
    #print(read_cluster_csv('./hadoop/cluster2.csv'))
