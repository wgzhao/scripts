source ../util/hadoop.sh

for((i=70;i<71;i+=1))
	do
today=`date -d -"$i"day +%Y-%m-%d`
echo "today: "$today
input=/bing/data/ods_app_actionlog_raw_dm/statis_date=$today/*
output=/user/zhangzhonghui/logcount/action/count
files=app_action.py,../column.py,app_action1.py,actionTime.py,readActivityDoc.py,activitydoc.txt
mapper="python,app_action.py,parser"
reducer="python,actionTime.py"

mrGroupsort $input $output $files $mapper $reducer

hdfs dfs -text /user/zhangzhonghui/logcount/action/count/p* | python actionTime.py merge > ~/data/action/time.$today

done

