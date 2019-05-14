
for((i=2;i<3;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		echo $today

		inputDir=/user/yarn/logs/source-log.php.CDA39907.resp/$today
		inputDir=$inputDir",/user/yarn/logs/source-log.php.CDA39907/$today"
		outputDir=/user/zhangzhonghui/logcount/tmp
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files test.py,../column.py \
		-D mapred.map.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=0 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python test.py" 

	done

