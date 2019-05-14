#!/bin/bash
## location: 10,dest=/data/crontab/,owner=weiguo:sysadmin,mode=755
# 将生产系统上的行为日志和nginx日志导入到HDFS
# usage: $0 [ date ] [action]
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin
time_offset=${1:-"1 day ago"}
logfile=/tmp/get_nginx.log
rundate=$(date -d "$time_offset" +"%Y-%m-%d")
RETVAL=0
if [ $? -ne 0 ];then
    echo "date format is invalid"
    exit 2
fi

#Nginx日志路径
NGINX_BASEDIR="/apps/hive/warehouse/backup"
nginx_logfile="nginx_${rundate}.log.tar.lzo"

#download nginx archiver from beijing
#baseurl="http://211.151.151.234/nginx"
BASEDIR="/online_logs/archive/nginx"

# 首先判断当前需要的归档文件是否已经生成，判断依据是是否在当前目录下已经产生.done文件
echo -n "waiting for archive log"
while true;
do
	if [ -f $BASEDIR/$nginx_logfile ];then
        break
    else
	    echo -n "."
	    sleep 1
    fi
done

echo

#axel -n 10 -s 10485760 -o $nginx_logfile $baseurl/$nginx_logfile > $logfile 2>&1


hdfs dfs -mkdir ${NGINX_BASEDIR}/${rundate}
#erase old data
hdfs dfs -test -f ${NGINX_BASEDIR}/${rundate}/$nginx_logfile
if [ $? -eq 0 ];then
    echo "file $nginx_logfile has exists,skip"
    exit 0
fi

#upload to hdfs
hdfs dfs -put $BASEDIR/${nginx_logfile} ${NGINX_BASEDIR}/${rundate}/
if [ $? -ne 0 ];then
    echo "failed to upload archiver "
    exit 5
fi
#create index for lzo format
hadoop jar /usr/lib/hadoop/lib/hadoop-lzo-0.6.0.jar com.hadoop.compression.lzo.LzoIndexer  ${NGINX_BASEDIR}/${rundate}/

exit 0
