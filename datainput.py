#!/usr/bin/python2.7  
# -*- coding: utf-8 -*- 

# 数据 输入与预处理相关功能

import pandas as pd
import numpy as np 
from utils import Job, QueueConfig, QueueWish


# CAPACITY_INDEX = 1 
# MAX_CAPACITY_INDEX = 3
# ABS_CAPACITY = 4
# QUEUE_NAME_INDEX = 8
# QUEUE_STATE_INDEX = 9
# PENDING_CONTAINERS = 15
# NUM_PENDING_APPLICATIONS = 19
def read_scheduler_csv(path):
    df = pd.read_csv(path)
    cols = df.columns.tolist()
    confs = [] 
    for index, row in df.iterrows():
        queue_config = QueueConfig()
        queue_config.capacity = float(row[1])
        queue_config.max_capacity = float(row[3])
        queue_config.abs_capacity = float(row[4])
        queue_config.pending = row[15]
        # print(row[15])
        queue_config.name = row[8]
        queue_config.state = row[9]
        confs.append(queue_config)
        """
        for i in range(df.shape[1]):
            print(i, cols[i], row[i])
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

# QUEUE_INDEX = 23
# ELAPSED_TIME_INDEX = 10
# MEMORY_SECONDS_INDEX = 15 
# MEMORY_ALLOCATE = 0
def read_app_csv(path):
    df = pd.read_csv(path)
    # cols = df.columns.tolist()
    jobs = []
    for index, row in df.iterrows():
        job = Job()
        job.name = row[23]
        job.run_time = row[10] * 0.001
        job.memory_seconds = row[0]*300
        jobs.append(job)
        """
        for i in range(df.shape[1]):
            print(i, cols[i], row[i])
        print( '-----------------------------------')
        """
    return jobs

# QUEUE_INDEX = 3
# START_TIME_INDEX = 14
# FINISHED_TIME_INDEX = 15
# ELAPSED_TIME_INDEX = 16
# MEMORY_SECONDS_INDEX = 22
def read_app_stopped_csv(path):
    df = pd.read_csv(path)
    # cols = df.columns.tolist()
    jobs = []
    for index, row in df.iterrows():
        job = Job()
        job.name = row[3]
        # job.wait_time = np.random.randint(50) #暂时用随机数模拟
        job.run_time = row[16] * 0.001
        job.memory_seconds = row[22]
        if job.run_time > 150:
            job.memory_seconds = job.memory_seconds * 150 / job.run_time
            print("STOPPED: ", job.memory_seconds)
        jobs.append(job)
        """
        for i in range(df.shape[1]):
            print(i, cols[i], row[i])
        print( '-----------------------------------')
        """
    return jobs

# QUEUE_INDEX = 23
# ELAPSED_TIME_INDEX = 10
# MEMORY_SECONDS_INDEX = 15 
def read_app_started_csv(path):
    # print("PATH:", path)
    df = pd.read_csv(path)
    #cols = df.columns.tolist()
    jobs = []
    for index, row in df.iterrows():
        job = Job()
        job.name = row[23]
        job.run_time = row[10] * 0.001
        job.memory_seconds = row[15]
        jobs.append(job)
        """
        for i in range(df.shape[1]):
            print(i, cols[i], row[i])
        print( '-----------------------------------')
        """
    return jobs

# 0, time_step
# 1, mem_predicion
# 2, cpu_prediction
# 3, queue_name
def read_prediction_csv(path):
    df = pd.read_csv(path)
    wishes = []
    for index, row in df.iterrows():
        wish = QueueWish() 
        wish.vmem = row[1]
        wish.vcpu = row[2]
        wish.name = row[3]
        wishes.append(wish)
    return wishes

if __name__ == '__main__':
    jobs = read_app_started_csv('../hadooputil/output/runningapp1.csv')
    """
    for i in range(len(jobs)):
        job = jobs[i]
        print(i)
        job.display()
    read_scheduler_csv('./hadoop/scheduler2.csv')
    #print(read_cluster_csv('./hadoop/cluster2.csv'))
    """
