# hadoop-yarn-score

This project was created for purpose of evaluating a hadoop system by scoring its yarn scheduler.

## Config
The config file in `./conf/config.json` should be edited before first start.

| Item | Description | 
| :----------- | :------: | 
|stat_interval      |Interval between two score period in second, which is used in calculating memeory usage, 2 hours = 7200 |   
|scheduler_metric_path |The path of scheduler infos gathered from hadoop cluster, default value is "./hadoop/scheduler2.csv". |
|job_metric_path |The path of app infos gathered from hadoop cluser, default value is "./hadoop/app.csv" |
|yarn_config_file    |The path of XML file exported yarn scheduler. For test, use "./conf/yarn_config.xml". For product, use "./conf_config_xian.xml" . |
|cluster_metric_path |Path of cluster infos gathered from hadoop, used for calclulating the abs_memory of eahc queue. Default value is "./hadoop/cluster2.csv" |
|prediction_path|Path of LSTM prediction result for each queue, default value is "./hadoop/prediction.csv" |
|stat_output_file |Path of output file, the result of scoring and prediction |
|sys_total_memory| A value for initialization the root queue memory size, default is 16384. It can be updated by the cluster info.|
|rest_port |Restful server will be listening on the port. default: 5001 |


## Start
Then type `python score.py` to start the program.
A restful server is started which will response following request:

| Request | description | 
| :----------- | :------: | 
| /update/all       | Update all information gathered from hadoop system   | 
| /score       | Scoring hadoop system by updated information    |
| /predict      | Update prediction from prediction.csv and give the new capacity scheme for next duration |


## Test
`python score_test.py`
Where all app/scheduler/queue info are generated randomly every 5 seconds.

Detailed readme will be updated later.
