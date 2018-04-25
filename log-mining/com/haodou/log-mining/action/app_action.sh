source ../util/hadoop.sh

if [ -z $1 ]; then
	echo "请指定日期！"
	exit 1
fi

today=$1
next=$(python ../util/TimeUtil.py addDay $today 1)
nnext=$(python ../util/TimeUtil.py addDay $today 2)
n3next=$(python ../util/TimeUtil.py addDay $today 3)

input=/user/yarn/logs/source-log.php.CDA39907/$today/*,/bing/data/ods_app_actionlog_raw_dm/statis_date=$today/*,/bing/data/ods_app_actionlog_raw_dm/statis_date=$next/*,/bing/data/ods_app_actionlog_raw_dm/statis_date=$nnext/*,/bing/data/ods_app_actionlog_raw_dm/statis_date=$n3next/*
output=/user/zhangzhonghui/logcount/action/tmp
files=app_action.py,../column.py,app_action1.py
mapper="python,app_action.py,merge"
reducer="cat"

mrGroupsort $input $output $files $mapper $reducer

input=$output
output=/user/zhangzhonghui/logcount/action/diff
files=app_action.py,../column.py,app_action1.py
mapper="python,app_action.py,diff"
reducer="python,app_action.py,diffMerge"

basic $input $output $files $mapper $reducer


