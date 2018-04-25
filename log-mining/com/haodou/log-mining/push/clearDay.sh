
#清除已经生成的某天数据

if [ -z $1 ]; then
	echo "未指定日期"
	exit 1
fi

day=$1

hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/user_mid/$day
hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/userTags/$day
hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/message/$day
rm -r /data/push_tag/$day

