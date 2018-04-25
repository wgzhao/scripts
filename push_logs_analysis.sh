#!/bin/bash
# get all push logs and analyzes them
# hive table logs.pushlogs
# create table logs.pushlogs(
# statistime timestamp comment "YYYY-mm-dd HH:MM:SS.ms",
# sid string comment "session id",
# state string comment  "session status,it among Opened,Replaced and Closed",
# ip string comment "ip address come from",
# node string comment "ejabbred node"
# )
# PARTITIONED BY (
#   statis_date timestamp)
# ROW FORMAT DELIMITED
#   FIELDS TERMINATED BY ','
# STORED AS INPUTFORMAT
#   'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
# OUTPUTFORMAT
#   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';


# create table logs.pushacceptlogs(
#    statistime  timestamp comment "YYYY-mm-dd HH:MM:SS.ms",
#    ip  string comment "ip address",
#    port   int comment "port connect to ",
#    node   string comment "ejabbred node"
#    )
#PARTITIONED BY (
#    statis_date string)
#ROW FORMAT DELIMITED
#    FIELDS TERMINATED BY ','
# STORED AS INPUTFORMAT
#   'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
# OUTPUTFORMAT
#   'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat';

#
# create table logs.pushauthfails(
# statistime  timestamp comment "YYYY-mm-dd HH:MM:SS.ms",
# sid     string comment "uuid",
# ip      string comment "ip address",
# node    string comment "ejabberd server node"
# )
# partitioned by (
# statis_date date
# )

#  节点列表
nodes=(202 203 205)
# 时间偏移量，默认是前一天
offset="1 day ago"
# pig 脚本文件
pigfile="/home/crontab/pushlogs2hive.pig"
# 归档文件下载URL
url="http://download.hoto.cn/push_log"

# 统计路径，rtc表示从实时日志统计,arch 表示从归档日志统计
statist="rtc"

# 执行功能，默认是下载+统计
method="all"

function usage(){
	echo "$0 [ -d <date> ] [-f <pigfile> ] -h"
	echo -e "-d <date> \t specify running date,defaults to yesterday"
	echo -e "-f <pigfile> \t pig script filepath,defaults to /home/crontab/pushlogs2hive.pig"
	echo -e "-h \t print usage and exit"
	exit 1
}

while getopts "d:f:h" opt
do
	case $opt in
		d) offset=$OPTARG
		;;
		f) pigfile=$OPTARG
		;;
		h|'*') usage
		;;
	esac
done

dashdate=$(date -d "$offset" +"%Y-%m-%d")
if [ $? -gt 0 ];then
    echo "$arg is invalid date format"
    exit 2
fi
isodate=$(date -d "$offset" +"%Y%m%d")

if [ ! -f "$pigfile" ];then
	echo "$pigfile not exists"
	exit 2
fi


# 归档日志目录
hdfsdir=hdfs://hdcluster/backup/push_logs/${dashdate}
# logs.pushlogs 表分区所在路径
hivepushlogsdir=hdfs://hdcluster/apps/hive/warehouse/logs.db/pushlogs/statis_date=${dashdate}
# logs.pushacceptlogs 表分区所在路径
hiveacceptlogsdir=hdfs://hdcluster/apps/hive/warehouse/logs.db/pushacceptlogs/statis_date=${dashdate}
# logs.pushauthfails 表分区所在路径
hivepushauthfailsdir=hdfs://hdcluster/apps/hive/warehouse/logs.db/pushauthfails/statis_date=${dashdate}
# return code
retcode=0

#add table partition
hive -S -e "use logs; alter table pushlogs  drop if exists partition(statis_date='${dashdate}'); alter table pushlogs add  partition (statis_date='${dashdate}'); alter table pushacceptlogs drop if exists partition(statis_date='${dashdate}'); alter table pushacceptlogs add partition(statis_date='${dashdate}');alter table pushauthfails drop if exists partition(statis_date='${dashdate}'); alter table pushauthfails add partition(statis_date='${dashdate}');alter table pushlogs partition(statis_date='${dashdate}') set fileformat TEXTFILE; alter table pushacceptlogs partition(statis_date='${dashdate}') set fileformat TEXTFILE;"

#dele partition path
hdfs dfs -rm -r $hivepushlogsdir $hiveacceptlogsdir $hivepushauthfailsdir
pig -b -param rtcdir="${hdfsdir}/" -param dashdate=${dashdate} -param pushlogstbl=$hivepushlogsdir -param acceptlogtbl=$hiveacceptlogsdir -param pushauthfailtbl=$hivepushauthfailsdir -f $pigfile
[ $? -eq 0 ] || retcode=1

/home/crontab/get_unknown_ips2.py ${dashdate}

exit $retcode
