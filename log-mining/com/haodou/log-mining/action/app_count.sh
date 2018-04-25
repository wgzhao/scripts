source ../util/hadoop.sh

for((i=2;i<3;i++))
	do
today=`date -d -"$i"day +%Y-%m-%d`

input=/bing/data/ods_app_actionlog_raw_dm/statis_date=$today/*
output=/user/zhangzhonghui/logcount/action/count
files=app_action.py,../column.py,app_action1.py
mapper="python,app_action.py,parser"
reducer="python,app_action.py,actionCount"

basic $input $output $files $mapper $reducer

hdfs dfs -text /user/zhangzhonghui/logcount/action/count/p* | python app_action.py countMerge> ~/data/action/count.$today

done

