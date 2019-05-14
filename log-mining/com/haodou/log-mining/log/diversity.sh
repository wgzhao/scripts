
tday=`date -d -1day +%m%d`
mkdir backup.$tday
for((i=1;i<40;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		midDir=/user/zhangzhonghui/logcount/diversity.count
		outputDir=/user/zhangzhonghui/logcount/diversity.cc
		
		hadoop fs -rm -r $midDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files diversity.py,diversityReduce.py,../column.py \
		-D mapred.map.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=10 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D stream.recordreader.compression=gzip \
		-input  $inputDir \
		-output $midDir \
		-mapper "python diversity.py" \
		-reducer "python diversityReduce.py"

		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ccReduce.py,ccinfo.py \
		-D mapred.map.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=1 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input $midDir \
		-output $outputDir \
		-mapper "cat" \
		-reducer "python ccReduce.py"

		hdfs dfs -cat $outputDir/* > backup.$tday/diversity.$today

	done

