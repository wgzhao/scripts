
		hdfs dfs -rm -r /user/zhangzhonghui/logcount/push/tmpUser/*			
		hdfs dfs -put user.txt /user/zhangzhonghui/logcount/push/tmpUser

		inputDir=/user/zhangzhonghui/logcount/push/user_mid/*/*,/user/zhangzhonghui/logcount/push/tmpUser
		outputDir=/user/zhangzhonghui/logcount/push/filterUserMid
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files filterUser.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=30 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "cat" \
		-reducer "python filterUser.py"


