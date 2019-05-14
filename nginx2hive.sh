#!/bin/zsh
# 从实时日志分析导入到hive中
rtcdir="/backup/nginx/"
#tbldir="hdfs://hdcluster/user/yarn/${hivedb}.
#tbldir="hdfs://hdcluster/apps/hive/warehouse/www_haodou_com"
offset=${1:-"now"}
pigfile=${2:-"/home/crontab/nginx2hive.pig"}
curday=$(date -d $offset +"%F")
hivedb="logs"
RETCODE=0
#scan result with similar as
#/rtc/nginx/qnc/api_qunachi_com/2014-10-12
logdirs=( $(hdfs dfs -ls -d ${rtcdir}/\*/\*/$curday | awk "/$curday/ {print \$NF}" |tr '\n' ' ') )
for log in $logdirs
do
    pattern=(${(s#/#)log})
    domain=$pattern[4]
        echo "process $domain log"
    #add partition for hive table
    hql="use ${hivedb}; alter table ${domain} add if not exists partition(logdate='$curday');"
    hive -S -e "$hql"
    if [ $? -ne 0 ];then
        echo "failed create partition for table ${hivedb}.${domain}"
		RETCODE=1
        continue
    fi
        saved="/tmp/${domain}"
        #remove old data
        hdfs dfs -rm -r -skipTrash $saved  >/dev/null 2>&1
        pig  -b --param log=$log --param saved=$saved -f  $pigfile
        hive -S -e "load data inpath '$saved' overwrite into table ${hivedb}.${domain} partition(logdate='$curday');"
done

exit $RETCODE