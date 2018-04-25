#!/bin/zsh
## location: 10,dest=/data/crontab,owner=weiguo:sysadmin,mode=755
#dump all online logs to hdfs
# 将北京生产环境的错误日志导入到HDFS，包括Nginx日志，PHP日志，MYSQL日志等
HDFS_BASEDIR="/online_logs/beijing"
LOGS_DIR="/home/rsync_online_logs"
if [ -z $1 ];then
    CURR_DAY=$(date -d "1 day ago" +"%Y%m%d")
else
	CURR_DAY=$(date -d "$1" +"%Y%m%d")
    if [ $? -ne 0 ];then
        echo "$1 is not valid date format"
        exit 1
    fi
fi

cd $LOGS_DIR || exit 1

for f in $(find . -type f -name \*_${CURR_DAY}.\* )
do
	#split filename into two pattern,take it to directory
	fdir=${f:h}
	fname=${f:t}
	p=( ${(s:_:)fname} )
	attdir=`echo ${p[0,-2]} |tr ' ' '_'`
	#get date 20140505.log
	d=$( echo ${p[-1]} |cut -d. -f1) 
	fdir="$fdir/$attdir/$d"
	echo $fdir/$fname
	#fpath is exists ?
	hdfs dfs -test -d ${HDFS_BASEDIR}/${fdir}
	[ $? -eq 0 ] || hdfs  dfs -mkdir -p ${HDFS_BASEDIR}/${fdir}
	#logfile is exists?
	hdfs dfs -test -f ${HDFS_BASEDIR}/${fdir}/${fname}
	[ $? -eq 0 ] || hdfs dfs -copyFromLocal $f ${HDFS_BASEDIR}/${fdir}
done

