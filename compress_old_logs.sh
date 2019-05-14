#!/bin/bash
# 对老的保存在HDFS上的日志文件进行压缩，以节约空间
# 脚本每天执行，压缩前一个月的日志
# 当前仅对/rtc目录
offset=${1:-"1 month ago"}
pdate=$(date -d "$offset" +"%F")
behaviorlogs=($(hdfs dfs -ls -d /rtc/behavior/*/$pdate/ | grep '/rtc' |awk '{print $NF}' |tr '\n' ' ' ))
for d in ${behaviorlogs[@]}
do
    hdfs dfs -cat ${d}/*.log |bzip2 |hdfs dfs  -put -f - ${d}/behavior.log.bzip2
    if [ $? -eq 0 ];then
        hdfs dfs -rm -r ${d}/*.log
    else
        echo "failed to compress $d "
    fi
done

nginxlogs=($(hdfs dfs -ls -d /rtc/nginx/*/*/$pdate/ | grep '/rtc' |awk '{print $NF}' |tr '\n' ' ' ))
for d in ${nginxlogs[@]}
do
    hdfs dfs -cat ${d}/*.log |bzip2 |hdfs dfs -put -f -  ${d}/nginx.log.bzip2
    if [ $? -eq 0 ];then
        hdfs dfs -rm -r ${d}/*.log
    else
        echo "failed to compress $d "
    fi
done

search_logs=($(hdfs dfs -ls -d /rtc/search_logs/$pdate/ | grep '/rtc' |awk '{print $NF}' |tr '\n' ' ' ))
for d in ${search_logs[@]}
do
    hdfs dfs -cat ${d}/*.log |bzip2 |hdfs dfs -put -f - ${d}/search.log.bzip2
    if [ $? -eq 0 ];then
        hdfs dfs -rm -r ${d}/*.log
    else
        echo "failed to compress $d "
    fi
done

push_logs=($(hdfs dfs -ls -d /rtc/push_logs/$pdate/ | grep '/rtc' |awk '{print $NF}' |tr '\n' ' ' ))
for d in ${search_logs[@]}
do
    hdfs dfs -cat ${d}/*.log |bzip2 |hdfs dfs  -put -f - ${d}/push.log.bzip2
    if [ $? -eq 0 ];then
        hdfs dfs -rm -r ${d}/*.log
    else
        echo "failed to compress $d "
    fi
done
