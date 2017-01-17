#!bin/bash

# Author: zhangsong5
# Date: 2017/01/17

CURR_DIR=$(pwd)
LD_DIR=$CURR_DIR/../lib_C
export LD_LIBRARY_PATH=$LD_DIR

MONITOR_BIN=$CURR_DIR/log_monitor 
LOG_FILE=../logs/impalad_node0.WARNING
MATCH_PATTERN=Cache_Subquery_SQL
SPLIT=\;
FILED_NUM=1

$MONITOR_BIN  $LOG_FILE $MATCH_PATTERN $SPLIT  $FILED_NUM
