source ../conf.sh

cd $root_local_dir/push

source ./getDate.sh $1 $1

if [ $? -ne 0 ];then
	echo "日期产生失败"
	exit 1
fi

mkdir $home_local_dir/data/push/$pushTargetDate
dataDir=$home_local_dir/data/push/$pushTargetDate

#2015年1月底从push_msg_android迁移到push_sended_android
hive -e "select to_user,packet,update_at from push_db.push_sended_android where day in ('$pushTargetDate','$nextDay')" > $dataDir/sendedLog.txt

if [ $? -ne 0 ]; then
	echo "读取发送日志失败!"
	exit 1
fi

#
#/backup/behaviour/push_received 如果直接从hdfs读取，可以读这里
hive -e "select uuid,message,appid,log_date,eventid from behavior.ods_app_push_received where log_date in ('$pushTargetDate','$nextDay')" > $dataDir/receiveLog.txt
if [ $? -ne 0 ]; then
	echo "读取接收日志失败!"
	exit 1
fi

hdfs dfs -text /backup/behaviour/pushview/$pushTargetDate/* > $dataDir/pushview.txt
if [ $? -ne 0 ]; then
	echo "读取点击日志失败!"
	exit 1
fi

if [ $nextDay != $pushTargetDate ]; then
	echo $nextDay
	hdfs dfs -text /backup/behaviour/pushview/$nextDay/* >> $dataDir/pushview.txt
	if [ $? -ne 0 ]; then
		echo "读取前天点击日志失败!"
		#exit 1
	fi
fi

hdfs dfs -test -e /user/zhangzhonghui/logcount/push/userTags/$pushTargetDate/
if [ $? -ne 0   ];then
	echo "读取用户分群数据失败！"
else
	hdfs dfs -text /user/zhangzhonghui/logcount/push/userTags/$pushTargetDate/* > $dataDir/userPolicy.txt
fi

cp defaultPushItem.txt $dataDir

cp itemBank.txt $dataDir


python readLog.py $pushTargetDate
if [ $? -ne 0 ]; then
	echo "合并日志失败!"
	exit 1
fi

