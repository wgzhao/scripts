
tday=`date -d -1day +%m%d`
mkdir backup.$tday
for((i=1;i<7;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		outputDir=/user/zhangzhonghui/logcount/method.count
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files methodInfo.py,infoReduce.py,infoReduce.sh,../column.py,dictInfo.py,ccinfo.py \
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
		-mapper "python methodInfo.py" \
		-reducer "sh infoReduce.sh"

		hdfs dfs -text $outputDir/* > backup.$tday/methodInfo.$today

	done

