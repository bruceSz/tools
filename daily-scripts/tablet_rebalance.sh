#!/bin/bash

#
# Author: zhangsong5
# Date: 2017/01/12
#

TS_CLI="/export/ldb/kudu-tools/bin/kudu-ts-cli"
KUDU_ADMIN="/export/ldb/kudu-tools/bin/kudu-admin"
KUDU_MASTER="172.22.94.12:7051"

if [ ! $# -eq 1 ];then
    echo "$0 ts_host:port"
    exit 1
fi

ts_addr=$1

ts_host=$(echo $ts_addr|awk -F: '{print $1}')
tablet_file=${ts_host}_tablets

$TS_CLI  --server_address=${ts_addr} list_tablets > $tablet_file
echo "Dump to ${tablet_file}"

ts_uuid=$($TS_CLI --server_address=${ts_addr} status|grep permanent_uuid|awk -F: '{print $2}')
ts_uuid=${ts_uuid## }
ts_uuid=${ts_uuid#\"}
ts_uuid=${ts_uuid%\"}
echo ts uuid: $ts_uuid

echo "begin to remove top 50 tablet(larger than 1G) to other kudu-tserver\n "
idx=0;
all_idx=0
for t in $(grep RUNNING  $tablet_file -C 3|grep G -B 4|grep "Tablet id" |awk -F: '{print $2}'|head -n 50);do
    ./kudu-admin -master_addresses $KUDU_MASTER change_config $t  REMOVE_SERVER $ts_uuid;
    if [  $? -eq 0 ];then
        idx=$((idx+1));
        echo %%%%%%%%% move no.${idx} tablet with uuid: $t %%%%%%%%%%%%%%%;
    fi;
    sleep 3;
    all_idx=$((all_idx+1))
done

echo "all $all_idx , remove total ${idx} tablet from ${ts_host}(uuid: $ts_uuid)"
