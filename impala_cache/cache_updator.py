#!/bin/env python

#
# Author: zhangsong5
# Date: 2017/01/9
#

CONFIG_NAME="./config.json"
REDIS_MASTER_BIN=""
is_exit = False

import json
import threading
import time
import signal
import sys

def timestamp_to_daystr(timestamp):
    timeArray = time.localtime(timestamp)
    daystr = time.strftime("%Y%m%d", timeArray)
    return daystr

def daystr_to_timestamp(day_str):
    timeArray = time.strptime(day_str, "%Y%m%d")
    timestamp = int(time.mktime(timeArray))
    return timestamp

def timestr_to_timestamp(time_str):
    timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timeArray))
    return timestamp


def timestamp_to_timestr(timestamp):
    timeArray = time.localtime(timestamp)
    timestr = time.strftime("%Y-%m-%d %H:%M:%S", timeArray)
    return timestr


def table_generator(table_meta):
    start_table_name = table_meta["start"]
    table_prefix_l = start_table_name.split("_")[0:-1]
    table_prefix_str = "_".join(table_prefix_l)
    table_date = start_table_name.split("_")[-1]
    interval = table_meta["period"]
    table_date_timestamp = daystr_to_timestamp(table_date)
    while True:
        curr_clock = yield table_prefix_str + "_" + timestamp_to_daystr(table_date_timestamp)
        if curr_clock != 0 and curr_clock % interval == 0:
            table_date_timestamp += interval



def time_generator(time_meta):
    start_time = time_meta["start"]
    interval = time_meta["period"]

    # normalize timestamp in config file.
    time_val = timestr_to_timestamp(start_time)
    while True:
        curr_clock = yield timestamp_to_timestr(time_val)
        if curr_clock != 0  and curr_clock % interval == 0:
            time_val += interval


def gcd(first, second):
    if first<second:
        first,second = second,first
    while second!=0:
        temp = first%second
        first = second
        second = temp
    return first


def run_in_background(sql_info, type_meta):
    template_sql = sql_info["sql"]
    n_conds = []
    if "timestamp_names" in sql_info:
        timestamp_names = sql_info["timestamp_names"]
        timestamp_metas = sql_info["timestamp"] 
        for index in range(len(timestamp_names)):
            meta = timestamp_metas[index]
            meta_d = {}
            meta_d["interval"] = meta["period"]
            meta_d["template_name"] = timestamp_names[index]
            meta_d["actual_parameter"] = time_generator(meta)
            meta_d["actual_parameter"].next()
            meta_d["init_val"] = meta["start"]
            n_conds.append(meta_d)

    if "table_names" in sql_info:
        table_names = sql_info["table_names"]
        table_metas = sql_info["table"]
        for index in range(len(table_names)):
            meta = table_metas[index]
            meta_d = {}
            meta_d["interval"] = meta["period"]
            meta_d["template_name"] = table_names[index]
            meta_d["actual_parameter"] = table_generator(meta)
            meta_d["actual_parameter"].next()
            meta_d["init_val"] = meta["start"]
            n_conds.append(meta_d)

    jump_gap = -1
    for cond in n_conds:
        if jump_gap == -1:
            jump_gap = cond["interval"]
        else:
            jump_gap = gcd(jump_gap, cond["interval"])

    index = 0
    global is_exit
    while True:
        # sleep for jump_gap
        if is_exit:
            break
        
        curr_sql = template_sql
        for cond in n_conds:
            curr_sql = curr_sql.replace(cond["template_name"], cond["actual_parameter"].send(index))
        index += jump_gap
        print curr_sql
        print 10*"#"
        time.sleep(1)

    print "end of thread"  

def signal_handler(signum, frame):
    global is_exit
    is_exit = True
    print "Get signal, will exit."
    #sys.exit(0)

def main():
    """
    """
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    threads = []
    with open(CONFIG_NAME, "r") as f:
        json_o = json.load(f)
        sqls = json_o["sqls"]
        type_meta = json_o["timestamp_type_meta"]
        for idx in range(len(sqls)):
            sql_info = sqls[idx]
            if idx == 3:
                threads.append(threading.Thread(target=run_in_background, args=(sql_info, type_meta)))

        for th in threads:
            th.setDaemon(True)
            th.start()

        while True:
            all_die = True
            for th in threads:
                if th.isAlive():
                    all_die = False
            if all_die:
                break
            
        print "Come here when all thread end, should not came here  normally"

def test_time_generator():
    meta1 = {}
    meta1["start"] = "2017-01-09 00:00:00"
    meta1["period"] = 3600
    g1 = time_generator(meta1)
    g1.next()
    idx = 0
    while True:
        time.sleep(0.3)
        print g1.send(idx)
        idx += 1200

def test_tab_generator():
    meta1 = {}
    meta1["start"] = "gdm_log_w_20170109"
    meta1["period"] = 86400
    g1 = table_generator(meta1)
    idx = 0
    g1.next()
    while True:
        time.sleep(0.1)
        print g1.send(idx)
        idx +=43200
    

def test():
    test_time_generator()
    test_tab_generator()

if __name__ == "__main__":
    """
        TODO: there is a bug left: time condition coule be : time >= a and time < a
    """
    main()
