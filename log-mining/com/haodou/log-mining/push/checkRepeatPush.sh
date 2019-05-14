
		
		inputDir=/user/zhangzhonghui/logcount/push/user_mid/*/*
		outputDir=/user/zhangzhonghui/logcount/push/history
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
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
		-reducer "cat"

hdfs dfs -cat /user/zhangzhonghui/logcount/push/history/* | python checkRepeatPush.py 


