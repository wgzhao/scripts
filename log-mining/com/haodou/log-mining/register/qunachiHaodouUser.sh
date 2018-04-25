

		inputDir=/user/yarn/logs/source-log.php.CDA39907/2014-*/*.lzo,/user/yarn/logs/source-log.http.data.logs.nginx.api_qunachi_com.log/logdate=2014-*/*
		outputDir=/user/zhangzhonghui/logcount/reg/haodouQunachiUser
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files qunachiHaodouUser.py,../column.py  \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python qunachiHaodouUser.py map" \
		-reducer "python qunachiHaodouUser.py reduce"

		hdfs dfs -text $outputDir/* | python qunachiHaodouUser.py merge > ~/data/shqu.txt

		#hdfs dfs -rm -r $outputDir

