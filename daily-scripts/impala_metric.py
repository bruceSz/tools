#! /usr/bin/env python

# Author : zhangsong5
# Date: 2016/11/2

import time
import commands

DEBUG=True
IPS_FILE = "./conf/impala_ips"

DEFAULT_IMPALA_PORT=25000
DEFAULT_METIC_DIR = "./logs/impala_metric_dump/"


def curr_time():
    if not hasattr(curr_time, "time_"):
        time_ = time.strftime("%Y%m%d%H%M%S", time.localtime())
    else:
        return time_

CURRENT_TIME = curr_time()

class MetricDumper:
    def __init__(self, impala_ip, impala_port ):
        self.impala_ip = impala_ip
        self.impala_port = impala_port
        self.url_prefix = "curl http://"
        self.url_suffix = "/metrics"

        c_time =  CURRENT_TIME
        # assert the prefix is already there
        file_path_prefix = DEFAULT_METIC_DIR + c_time + "/" 
        file_path = file_path_prefix + impala_ip
        self.output = open(file_path, "w")


    def dump_metric(self):
        cmd = self.url_prefix+ self.impala_ip + ":" + self.impala_port + self.url_suffix 
        
        status, outout = commands.getstatusoutput(cmd) 
        if status != 0:
            print "Get metrics from " + self.impala_ip + ":" + self.impala_port + " failed"
            return 
        if self.output is None:
            print output
        else: 
            print >> output , output


def dump_impala_metric(impala_ip):
        
    md = MetricDumper(impala_ip, DEFAULT_IMPALA_PORT)
    md.dump_metric()
    


def iter_file(filename):
    with open(filename ) as f:
        for line in f:
            yield line.strip()
            if DEBUG:
                break
            

def init():


if __name__ == "__main__":
    init()
    for i in iter_file(IPS_FILE):
        print i
