#!/bin/sh
offset=${1:-"yesterday"}
curday=$(date -d $offset +"%F")
echo $offset
echo $curday
hive -e "load data inpath 'hdfs://hdcluster/rtc/pig/${curday}/pig.log' into table pig.ods_m001 partition (ptdate='${curday}')";
