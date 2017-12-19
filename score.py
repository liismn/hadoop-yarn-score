from __future__ import print_function
import os, time
import argparse
import resource_manager 
import conf
import datainput

FLAGS = None

def update_filepath(cfg):
    cluster_file = cfg.get_cluster_metric_path()
    scheduler_file = cfg.get_scheduler_metric_path()
    app_file = cfg.get_job_metric_path()
    return cluster_file, scheduler_file, app_file

# get the last update time of the file
def get_mtime(filename):
    info = os.stat(filename)
    return info.st_mtime

# parse csv file which includes queue info gathered from yarn scheduler
def update_scheduler_info(rmq, cfg):
    scheduler_file = cfg.get_scheduler_metric_path()
    ts = get_mtime(scheduler_file)
    # if True or ts > cfg.get_scheduler_timestamp():
    # if ts > cfg.get_scheduler_timestamp():
    queue_configs = datainput.read_scheduler_csv(scheduler_file) 
    for qc in queue_configs:
        queue = rmq.get_queue(qc.name)
        if queue is None:
            print("Unknown queue name", qc.name)
            continue 
        queue.data.update_queue_config(qc)
        #print(qc.name)
    #   cfg.update_scheduler_timestamp(ts)


# parse csv file which includes app info gathered from yarn scheduler
def update_app_info(rmq, cfg):
    app_file = cfg.get_job_metric_path()
    # ts = get_mtime(app_file)
    # if True or ts > cfg.get_job_timestamp():
    # if ts > cfg.get_job_timestamp():
    jobs = datainput.read_app_csv(app_file) 
    # if ts > cfg.get_app_timestamp():
    for job in jobs:
        queue = rmq.get_queue(job.name)
        if queue is None:
            print("Unkonw queue name", job.name)
            continue 
        queue.data.add_job(job)
    #   cfg.update_job_timestamp(ts)

# parse csv file which includes prediction info 
def update_prediction_info(rmq, cfg):
    prediction_file = cfg.get_prediction_file()
    wishes = datainput.read_prediction_csv(prediction_file) 
    for wish in wishes:
        queue = rmq.get_queue(wish.name)
        if queue is None:
            print("Unknow queue name", wish.name)
            continue
        queue.data.update_wish(wish)

# parse csv file which includes the total memory of root queue 
def update_cluster_info(rmq, cfg):
    cluster_file = cfg.get_cluster_metric_path()
    print(cluster_file)
    ts = get_mtime(cluster_file)
    totalMb = datainput.read_cluster_csv(cluster_file) 
    if totalMb == 0:
        return
    queue = rmq.get_queue('root')
    queue.data.add_totalMb(totalMb)
    # cfg.update_cluster_timestamp(ts)
    # queue.data.cal_totalMb_mean()

# parse csv file which includes the prediction of each leaf queue
def update_predict_info(rmq, cfg):
    prediction_file = cfg.get_prediction_path()
    queue_wishes = datainput.read_prediction_csv(prediction_file)
    for wish in queue_wishes:
        queue = rmq.get_queue(wish.name)
        if queue is None:
            print("Unkonw queue name", wish.name)
            continue 
        queue.data.update_queue_wish(wish)
        


def update_info(rmq, cfg):
    update_scheduler_info(rmq, cfg)
    update_cluster_info(rmq, cfg)
    update_app_info(rmq, cfg)

def score(rmq, cfg):
    rmq.score()
    rmq.display_score()
    path = cfg.get_stat_output_file()
    rmq.write_score(path)

def predict(rmq, cfg):
    update_predict_info(rmq, cfg)
    rmq.predict()
    rmq.display_prediction()
    path = cfg.get_stat_output_file()
    rmq.write_prediction(path)

def start(cfg):
    cluster_file, scheduler_file, app_file = update_filepath(cfg)
    rmq = resource_manager.parseYarnConfig(cfg.yarn_config_path)
    rmq.set_stat_interval(cfg.get_stat_interval())
    rmq.set_system_memory(cfg.get_sys_total_memory())
    print('Starting to collecting and scoring ... ')
    import restserver
    restserver.start_server(cfg.get_rest_port())
    """
    while True:
        update_scheduler_info(rmq, cfg) 
        update_cluster_info(rmq, cfg) 
        update_app_info(rmq, cfg) 
        now = time.time()
        if now >= cfg.get_next_stat_time():
            cfg.update_next_stat_time()
            rmq.score()
            rmq.display_score()
            pass
        time.sleep(cfg.get_update_interval()) 
    """
    
def main(config_path):
    cfg = conf.Config(config_path)
    print(cfg.config_file_path)
    cfg.update_config()
    print(cfg.get_cluster_metric_path())
    test_cfg = conf.Config()
    # cfg.init_all_timestamp()
    start(cfg)


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.register("type", "bool", lambda v: v.lower() == "true")
    parser.add_argument(
        "--config_file",
        type=str,
        default="./conf/config.json",
        help="The path of config file, in json format"
    )

    FLAGS = parser.parse_args()
    config_path = FLAGS.config_file

    main(config_path)
