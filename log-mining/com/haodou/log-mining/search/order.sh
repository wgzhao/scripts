
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/2014-07-*,/user/yarn/logs/source-log.php.CDA39907/2014-06-*
		outputDir=/user/zhangzhonghui/logcount/order
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files order.py,../column.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=1 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python order.py map" \
		-reducer "python order.py reduce"

		hdfs dfs -cat $outputDir/* > ~/data/order.txt

		hdfs dfs -rm -r $outputDir

