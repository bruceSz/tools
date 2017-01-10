#!/bin/env python

#
# Author: zhangsong5
# Date: 2017/01/9
#

CONFIG_NAME="./config.json"
REDIS_MASTER_BIN=""

import json
import threading
import time

def timestr_to_timestamp(time_str):
    timeArray = time.strptime(a, "%Y-%m-%d %H:%M:%S")
    timeStamp = int(time.mktime(timeArray))
    return timeStamp

def timestamp_to_timestr(timestamp):
    timeArray = time.localtime(timeStamp)
    otherStyleTime = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)

def time_generator(time_meta):
    start_time = time_meta["start"]
    interval = time_meta["period"]

    # normalize timestamp in config file.
    time_val = timestr_to_timestamp(start_time)
    while True:
        yield timestamp_to_timestr(time_val)
        time_val += interval


def gcd(first, second):
    if a<b:
        a,b = b,a
    while b!=0:
        temp = a%b
        a = b
        b = temp
    return a


def run_in_background(sql_info, type_meta):
    template_sql = sql_info["sql"]
    conditions = []
    if "timestamp_names" in sql_info:
        conditions.append({})

        timestamp_names = sql_info["timestamp_names"]
        timestamp_metas = sql_info["timestamp"] 
        for index in range(len(timestamp_names)):
            meta = timestamp_metas[index]
            print timestamp_names[index]


        


def main():
    """
    """
    threads = []
    with open(CONFIG_NAME, "r") as f:
        json_o = json.load(f)
        sqls = json_o["sqls"]
        type_meta = json_o["timestamp_type_meta"]
        for sql_info in  sqls:
            threads.append(threading.Thread(target=run_in_background, args=(sql_info, type_meta)))

        for th in threads:
            th.start()

        for th in threads:
            th.join()
        print "Come here when all thread end, should not came here  normally"


if __name__ == "__main__":
    main()
    
