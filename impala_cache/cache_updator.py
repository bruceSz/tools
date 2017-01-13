#!/bin/env python

#
# Author: zhangsong5
# Date: 2017/01/9
#

CONFIG_NAME="./config.json"
REDIS_MASTER_BIN=""

START_TIME = None

TEMPLATE_REDIS_CREATE_TABLE='''CREATE TABLE redis_table_name( redis_table_cols ) STORED AS TEXTFILE  TBLPROPERTIES ('__IMPALA_REDIS_LOCATION'='172.22.99.82:6380', '__IMPALA_REDIS_TABLE_METADATA_KEY'='redis_internal_table_name','__IMPALA_REDIS_CLASS'='redis_table', '__IMPALA_REDIS_API_VERSION'='V1', '__IMPALA_REDIS_INIT_STRING'='redis_md5_str')'''
META_REDIS_TABLE="redis_internal_table_name"
REDIS_TABLE="redis_table_name"
REDIS_TABLE_COLS="redis_table_cols"
REDIS_TABLE_MD5_STR="redis_md5_str"

is_exit = False

import logging
import logging.handlers
import json
import threading
import time
import signal
import sys
import hashlib
import commands

class RedisTableManager:
    def __init__(self, logger):
        self.cmd_prefix = "sh $IMPALA_HOME/bin/impala-shell.sh   -i 172.22.99.80:21000 -q "
        self.cmd_suffix = " --print_raw 2>/dev/null  "
        self.sql_prefix = "\"use test; "
        self.sql_suffix = " \""
        self.logger = logger

        self.redis_master_bin = "./redis-master"

    def get_redis_tab_size(self, tab_name):
        sql = "\"use test;"
        sql += "select count(1) from "
        sql += tab_name
        sql += "\""
        ret = self.exec_sql(sql).strip("[").strip("]").strip("'")
        return int(ret)

    def clear_redis_data(self, tab_name):
        cmd = "export LD_LIBRARY_PATH=./lib64 && "
        cmd += self.redis_master_bin 
        cmd += " delete_table "
        cmd += tab_name
        cmd += " -redis_meta_server_addr 172.22.99.82 -redis_meta_server_port 6380"
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            self.logger.warning(status)
            self.logger.warning(cmd)
            sys.exit(1)
        print output
        
    def exchange_table_name(self, tab1, tab2):
        sql = "\" use test;"
        sql += " alter table  " 
        sql += tab1
        sql += " rename to "
        sql += tab1 + "_tmp;"
        sql += " alter table " + tab2 + " rename to " + tab1 +";"
        sql += "alter table " + tab1 + "_tmp"+ " rename to " + tab2
        sql += "\""
        self.exec_sql(sql)

    def delete_redis_tab(self, tab_name):
        sql = "\" use test;"
        sql += "drop table if exists "
        sql += tab_name
        sql += "\""
        self.exec_sql(sql)
        self.clear_redis_data(tab_name)

    def ingest_data_into_redis(self, redis_table, select_sql):
        sql = "\"use jingo_recommendation;"
        sql += " insert into "
        sql += "test."
        sql += redis_table
        sql += " "
        sql += repr(select_sql)[2:-1]
        sql += "\""
        self.exec_sql(sql)

        
    def exec_sql(self, sql):
        cmd = self.cmd_prefix
        cmd += sql
        cmd += self.cmd_suffix
        status, output = commands.getstatusoutput(cmd)
        if status != 0:
            self.logger.warning(status)
            self.logger.warning(cmd)
            sys.exit(1)
        return output.replace("Using existing version.info file.","").strip()

    def createRedisTable(self, cols, table_name, md5_str):
        template_sql = TEMPLATE_REDIS_CREATE_TABLE       
        template_sql = template_sql.replace(REDIS_TABLE_COLS, cols)
        template_sql = template_sql.replace(REDIS_TABLE, table_name)
        template_sql = template_sql.replace(META_REDIS_TABLE, "__META__" + table_name)
        template_sql = template_sql.replace(REDIS_TABLE_MD5_STR,md5_str)
        sql = self.sql_prefix
        sql += template_sql
        sql += self.sql_suffix
        self.exec_sql(sql)



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

def table_to_timestamp(table_name):
    """
       Table has format like: gdm_log_w_20170109
    """
    day_str = table_name.split("_")[-1]
    return daystr_to_timestamp(day_str)


def curr_clock_timestamp():
    return int(time.time())

def start_timestamp():
    global  START_TIME
    if START_TIME is None:
        START_TIME = int(time.time())

    return START_TIME
    

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
    curr_time = start_timestamp()
    num_intervals = (curr_time - table_date_timestamp)/interval
    table_date_timestamp +=  interval * num_intervals
    while True:
        curr_clock = yield table_prefix_str + "_" + timestamp_to_daystr(table_date_timestamp)
        num_intervals = (curr_clock - table_date_timestamp)/interval
        table_date_timestamp +=  interval * num_intervals
        #if curr_clock != 0 and curr_clock % interval == 0:
        #    table_date_timestamp += interval



def time_generator(time_meta):
    start_time = time_meta["start"]
    interval = time_meta["period"]

    # normalize timestamp in config file.
    time_val = timestr_to_timestamp(start_time)
    curr_time = start_timestamp()
    num_intervals = (curr_time - time_val)/interval
    time_val += interval * num_intervals

    while True:
        curr_clock = yield timestamp_to_timestr(time_val)
        num_intervals = (curr_clock - time_val)/interval
        time_val += interval * num_intervals


def gcd(first, second):
    if first<second:
        first,second = second,first
    while second!=0:
        temp = first%second
        first = second
        second = temp
    return first


def md5_computer(in_str):
    hash_md5 = hashlib.md5(in_str)
    
    return hash_md5.hexdigest()


def run_in_background(logger, sql_info, type_meta):
    template_sql = sql_info["sql"]
    # template_sql = template_sql.decode('unicode_escape').encode('unicode_escape')
    redis_cols = sql_info["redis_structure"]
    redis_tab_name = "redis_" + md5_computer(redis_cols)
    initialized = False
    rtm = RedisTableManager(logger)
    rtm.delete_redis_tab(redis_tab_name)

    n_conds = []
    min_time = start_timestamp()
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
            #if timestr_to_timestamp(meta_d["init_val"]) < min_time:
            #    min_time = timestr_to_timestamp(meta_d["init_val"])
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
            #if table_to_timestamp(meta_d["init_val"]) < min_time:
            #    min_time = table_to_timestamp(meta_d["init_val"])

            n_conds.append(meta_d)

    jump_gap = -1
    for cond in n_conds:
        if jump_gap == -1:
            jump_gap = cond["interval"]
        else:
            jump_gap = gcd(jump_gap, cond["interval"])

    index = start_timestamp()#start_timestamp() - min_time
    global is_exit

    while True:
        # sleep for jump_gap
        if is_exit:
            break
        
        curr_sql = template_sql
        for cond in n_conds:
            curr_sql = curr_sql.replace(cond["template_name"], cond["actual_parameter"].send(index))

        cache_sql_md5 = md5_computer(curr_sql)
        if initialized:
            new_tab = redis_tab_name + "_new"
        else:
            new_tab = redis_tab_name
            
        logger.info(curr_sql)
        rtm.createRedisTable(redis_cols, new_tab, cache_sql_md5)
        rtm.ingest_data_into_redis(new_tab, curr_sql)
        new_tab_s = rtm.get_redis_tab_size(new_tab)

        if new_tab_s != 0:
            if initialized:
                rtm.exchange_table_name(new_tab, redis_tab_name)
                rtm.delete_redis_tab(new_tab)
            else:
                initialized = True


        # there maybe a time drift, from obvervation 0.13s / hour
        time.sleep(jump_gap)
        index = curr_clock_timestamp()
        #index += jump_gap

    print "end of thread"  

def signal_handler(signum, frame):
    global is_exit
    is_exit = True
    print "Get signal, will exit."
    #sys.exit(0)

def main():
    """
    """
    logger = setup_log()
    signal.signal(signal.SIGINT, signal_handler)
    signal.signal(signal.SIGTERM, signal_handler)

    threads = []
    with open(CONFIG_NAME, "r") as f:
        json_o = json.load(f)
        sqls = json_o["sqls"]
        type_meta = json_o["timestamp_type_meta"]
        for idx in range(len(sqls)):
            sql_info = sqls[idx]
            if idx == 2:
                threads.append(threading.Thread(target=run_in_background, args=(logger , sql_info, type_meta)))

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
    

def test_generator():
    test_time_generator()
    test_tab_generator()

def test_RedisTable():
    rtm = RedisTableManager() 
    cols = "id int, name string, sku_id bigint "
    tab_name = "redis_1_for_test"
    tab_name_2 = "redis_1_for_test_test_exchange"
    rtm.delete_redis_tab(tab_name)
    rtm.delete_redis_tab(tab_name_2)
    rtm.createRedisTable(cols, tab_name, "md5:12345678")
    #rtm.get_redis_tab_size(tab_name)
    rtm.ingest_data_into_redis(tab_name, "values (1, 'this is a name',567788)")
    print rtm.get_redis_tab_size(tab_name)
    rtm.createRedisTable(cols, tab_name_2, "md5:87654321")
    rtm.exchange_table_name(tab_name, tab_name_2)

def setup_log():
    LOG_FILE = 'logs/cache_updator.log'
    handler = logging.handlers.RotatingFileHandler(LOG_FILE, maxBytes = 1024*1024*1024, backupCount=3)
    fmt = '%(asctime)s - %(filename)s:%(lineno)s - %(name)s - %(message)s'
    formatter = logging.Formatter(fmt)
    handler.setFormatter(formatter)
    logger = logging.getLogger('cache_updator')
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)
    return logger
            

if __name__ == "__main__":
    """
        TODO: there is a bug left: time condition coule be : time >= a and time < a
    """
    #test_RedisTable()
    main()
