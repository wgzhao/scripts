source ../util/hadoop.sh

for((i=2;i<50;i+=5))
	do
today=`date -d -"$i"day +%Y-%m-%d`

input=/bing/data/ods_app_actionlog_raw_dm/statis_date=$today/*
output=/user/zhangzhonghui/logcount/action/count
files=app_action.py,../column.py,app_action1.py
mapper="python,app_action.py,netInVideo"
reducer="python,app_action.py,videoNetMerge"

basicNoReduce $input $output $files $mapper


hdfs dfs -text /user/zhangzhonghui/logcount/action/count/p* | python app_action.py videoNetMerge> ~/data/action/videoNet.$today

done

