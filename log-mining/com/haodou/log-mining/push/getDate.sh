
if [ -z $1 ];then
	echo "未指定评估日期!!"
	exit 1
fi

pushTargetDate=$1
nextDay=$(python dateAdd.py $pushTargetDate 1)
yesterday=`date -d -1day +%Y-%m-%d`
if [[ $nextDay > $yesterday ]]; then
	nextDay=$pushTargetDate
fi
if [ ! -z $2 ]; then
	nextDay=$2
fi

echo $pushTargetDate $nextDay

#hive -e "select to_user,packet,update_at from push_db.push_msg_android where update_at between '"+$pushTargetDate+" 15:00:00' and "+$nextDay+"' 00:00:00'" > ~/data/push/receiveLog.txt


