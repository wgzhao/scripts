#!/bin/zsh
#清理不再需要的老的hive数据库，主要是从MySQL导入过来的三个实例的数据库。
#每个月只保留第一天的，其他的全部删除。
#该脚本每月第一天运行，删除之前第4个月的非第一天数据
#比如，如果2014-10-01运行该脚本，则删除2014-06月的数据库但是保留2014-06-01的数据库
offset=${1:-"4 month ago"}
oldmonth=$(date -d "$offset" +"%Y%m")
#oldmonth=$(date -v-4m   +"%Y%m")
#获取指定月份的所有书库名
hqlfile="/tmp/drop_old_db.hql"
rm -f $hqlfile
dbs=($(hive -S -e "show databases like '*_${oldmonth}*'" 2>/dev/null |grep -v '^WARN' ) )
for db in $dbs
do
    part=(${(s/_/)db})
    db_day=$part[-1]
    if [ "$db_day" = "${oldmonth}01" ];then
        echo "$db remains "
        continue
    else
        echo "drop database ${db} cascade;" >>$hqlfile
    fi
done
#hive -S -f $hqlfile

#清理类似www_haodou_com表的老分区，只保留最近3个月的表分区
echo "use logs;" >>$hqlfile
hive -S -e "use logs; show tables like '*_com'" >/tmp/tbl.list
for tbl in `cat /tmp/tbl.list`
do
    m=$(date -d "$offset" +"%Y-%m")
    #remain first day every month
    for p in $(hive -S -e "use logs; show partitions $tbl" 2>/dev/null |grep -v '^WARN' |grep "$m" |grep -v "$m-01" |awk -F= '{print $2}')
    do
        echo "alter table $tbl drop  if exists partition(logdate='$p');" >>$hqlfile
    done
done

hive -f $hqlfile
exit $?
