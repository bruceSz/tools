#!/bin/env python

#
# Author: zhangsong5
# Date: 2017/01/19
#

import time

_START_TIME = None

class TimeUnit:
	SEC = 1
	MILLISECOND = 2


def timeunitstr2enum(unit):
	if unit.lower() == "second" :
		return TimeUnit.SEC
	elif unit.lower() == "millisecond":
		return TimeUnit.MILLISECOND
	else:
		assert(False)

def start_timestamp(unit=TimeUnit.SEC):
    global  _START_TIME
    if _START_TIME is None:
        _START_TIME = int(time.time())

    if unit == TimeUnit.SEC:
    	return _START_TIME
    elif unit == TimeUnit.MILLISECOND:
    	return 1000 * _START_TIME
    else:
    	assert(False)


def normalize_timestamp(timestamp, unit = TimeUnit.SEC):
	if unit == TimeUnit.SEC:
		return timestamp
	elif unit == TimeUnit.MILLISECOND:
		RETURN 1000 * timestamp
	else:
		assert(False)

# unit of timestamp is second
def timestr_to_timestamp(time_str, unit=TimeUnit.SEC):
    timeArray = time.strptime(time_str, "%Y-%m-%d %H:%M:%S")
    timestamp = int(time.mktime(timeArray))
    if unit == TimeUnit.SEC:
    	return timestamp
    elif unit == TimeUnit.MILLISECOND:
    	return 1000 * timestamp
    else:
    	assert(False)