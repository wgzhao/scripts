#!/bin/bash
# 给hive表增加分区，并设置分区的文件格式为纯文本
# 该方法主要是提供给那些实时写入表分区数据的表来使用的
# Author: zhaoweiguo<zhaoweiguo@haodou.com>
# Date: 2015-02-05
##############################################

function usage() {
    echo "$0 add partitons for specifed table"
    echo "$0 < -s start date>  < -e end date> < -d database >  < -t table > [-k partition key ][ -n ] [-h] "
    echo """usage:
        -s start date,like 2015-01-01 
        -e end date ,like 2015-02-01
        -d hive database name
        -t hive table on database
        -k partition key,default to statis_date
        -n dry run,just print hql but not execute it 
        -h print usage
        """
    exit 1
}
DRYRUN=0
pkey="statis_date"

while getopts "s:e:d:t:k:nh" 2>/dev/null opt
do
    case $opt in 
        s) tmpsdate=$OPTARG
        ;;
        e) tmpedate=$OPTARG
        ;;
        d) dbname=$OPTARG
        ;;
        t) tblname=$OPTARG
        ;;
        k) pkey=$OPTARG
        ;;
        n) DRYRUN=1
        ;;
        h) usage
        ;;
        '*') usage
        ;;
    esac
done

sdate=$(date -d "$tmpsdate" +"%F" )
if [ $? -ne 0 ];then
    echo "$tmpsdate is not valid date format"
    exit 2
fi
edate=$(date -d "$tmpedate" +"%F")
if [ $? -ne 0 ];then
    echo "$tmpedate is not valid date format"
    exit 3
fi
#end date must later than start date
sts=$(date -d "$sdate" +"%s")
ets=$(date -d "$edate" +"%s")
if [ $ets -lt $sts ];then
    echo "end date must be later than start date"
    exit 4
fi
tmpfile=$(mktemp)
echo "use $dbname;" >$tmpfile

rundate=$sdate
cnt=0
while [ "$rundate" != "$edate" ];
do
    echo "alter table $tbl add if not exists partition($pkey='$rundate');" >>$tmpfile
    echo "alter table $tbl partition($pkey='$rundate') set fileformat textfile;" >>$tmpfile
    rundate=$(date -d "$sdate + $cnt days" +"%F")
    (( cnt = cnt + 1 ))
done

if [ "$DRYRUN" == "1" ];then
    cat $tmpfile
    exit 0
else
    hive -f $tmpfile
    exit $?
fi


    