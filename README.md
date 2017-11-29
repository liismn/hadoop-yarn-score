# hadoop-yarn-score

This project was created for purpose of evaluating a hadoop system by scoring its yarn scheduler.

The config file in `./conf/config.json` should be edited before first start.

Then type `python score.py` to start the program.
A restful server is started which will response following request:

| Request | description | 
| :----------- | :------: | 
| /update/all       | Update all information gathered from hadoop system   | 
| /score       | Scoring hadoop system by updated information    |
| /predict      | Update prediction from prediction.csv and give the new capacity scheme for next duration |

Detailed readme will be updated later.
