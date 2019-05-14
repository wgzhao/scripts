#!/bin/bash
## location: 10,dest=/data/crontab,owner=weiguo:sysadmin,mode=755
#dump all online logs to hdfs
# 将行为日志导 入到HDFS
## 2015-08-04
## 日志已经实时存储到10.1.1.60节点上
## 因此该脚本仅用来检查数据是否已经保存在
HDFS_BASEDIR="/backup/behaviour"
LOCAL_BASEDIR="/logs/bi_exp"
if [ -z $1 ];then
    CURR_DAY=$(date -d "1 days ago"  +"%Y-%m-%d")
else
	CURR_DAY=$(date -d "$1" +"%Y-%m-%d")
fi

hdfs dfs -test -d /backup/behaviour/app_action/$CURR_DAY

if [ $? -eq 0 ];then
    echo "data has exists"
    exit 0
else
    echo "data has not exists"
    exit 1
fi

cd $LOCAL_BASEDIR/$CURR_DAY || exit 1
#first compress all log file
lzop -U *.log

#rsync -av rsync://211.151.151.237/behaviour/\*.${CURR_DAY}.lzo $LOGS_DIR/ 2>/tmp/rsync_behaviour.log 
#rsync -av /tmp/2014-07-09/*.${CURR_DAY}.gz $LOGS_DIR/ 2>/tmp/rsync_behaviour.log 

for f in *.lzo
do
	#break filename into 4 parts ,filename is such as app_action-info-211.151.151.237.log.lzo
    method=$(echo $f |cut -d- -f1)
	hdfsdir=${HDFS_BASEDIR}/${method}/${CURR_DAY}
    hdfs dfs -mkdir -p ${hdfsdir}
    hdfs dfs -moveFromLocal $f ${hdfsdir}/
    #hadoop jar /usr/lib/hadoop/lib/hadoop-lzo-0.6.0.jar com.hadoop.compression.lzo.LzoIndexer ${hdfsdir}/
done



