#!/bin/bash
# 快速hive数据库clone
##
# 该脚本的来源是：
# 有的时候mysql2hive.py脚本没有把一些表或者库导入导入到Hive，如果重新导入，则成本比较高。因为是每天都是一个全量。
#  如果有已经导入的更新的表或者库，则可以直接克隆这个表或者库
# 比如hd_haodou_20141030的库是完整的，而hd_haodou_20141025的库不完整，则数据上可以把前者的拷贝到后者。
# 然后把后者需要的表重建一遍即可。
# 这种方式比用sqoop重新导入数据库，效率要高很多
##
function usage() {
    echo "$0 [-p <database path prefix> ] [ -d <database> ] [ -s <source day> ]  [ -t <target day> ] [-h]"
    echo -e "-p <database path prefix> \t the basedir in HDFS for hive db"
    echo -e "-d <database> \t database name ,like hd_haodou_admin "
    echo -e "-d <source day> \t date for source database ,default is 20141030"
    echo -e "-t <target day> \t date for target database, like 20141024\n"
    echo -e "-h \t\t print usage then exit"
    echo -e "\t e.g $0 /apps/hive/warehouse/haodou_warehouse haodou 20141030 20141024 "
    echo -e "\t means it will copy /apps/hive/warehouse/haodou_warehouse/hd_haodou_20141030 to /apps/hive/warehouse/haodou_warehouse/hd_haodou_20141024 then create all missing tables in hd_haodou_20141024"
    exit 1
}
#default option
srcday=$(date +"%Y%m%d")
hdfsdir="/apps/hive/warehouse/mysql_warehouse"
while getopts "p:d:s:t:h" opt
do
        case $opt in
        p) hdfsdir=$OPTARG
        ;;
        d) dbname=$OPTARG
        ;;
        s) srcday=$(date -d "$OPTARG" +"%Y%m%d")
        ;;
        t) targetday=$(date -d "$OPTARG" +"%Y%m%d")
        ;;
        h) usage
        ;;
        esac
done

if [ -z "$hdfsdir" -o -z "$dbname" -o -z "$targetday" ];then
    usage
fi
dbs=$(hive -S -e "show database like *_${srcday}" |grep -v '^WARN')
for srcdb in ${dbs[@]}
do
  targetdb=${srcdb%%$srcday}${targetday}
  hadoop distcp -update  ${hdfsdir}/${srcdb} ${hdfsdir}/${targetdb}
  tbls=$(hive -S -e "use $srcdb; show tables")
  sql="create database if not exists ${targetdb}; use ${targetdb};"
  for tbl in ${tbls[@]}
  do
      sql="$sql create table if not exists \`${tbl}\` like ${srcdb}.\`${tbl}\`; "
  done

  hive -S -e "$sql"
done
exit $?
