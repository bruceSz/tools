#!/bin/env python

#
# Author: zhangsong5
# Date: 2017/01/19
#

import utils

def timestamp_generator(time_meta):
    start_time = time_meta["start"]
    interval = time_meta["period"]
    offset = time_meta["offset"]
    # number of unit based on second
    time_unit = timeunitstr2enum(time_meta["unit"])

    # normalize timestamp in config file.
    time_val = utils.timestr_to_timestamp(start_time)
    curr_time = utils.start_timestamp()
    num_intervals = (curr_time - time_val)/interval
    time_val += interval * num_intervals
    time_val += offset

    while True:
        curr_clock = yield utils.normalize_timestamp(time_val, time_unit)
        num_intervals = (curr_clock - time_val)/interval
        time_val += interval * num_intervals