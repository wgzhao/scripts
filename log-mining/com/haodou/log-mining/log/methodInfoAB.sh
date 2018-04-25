for((i=1;i<2;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		outputDir=/user/zhangzhonghui/logcount/methodInfoAB
		
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files dictInfo.py,../abtest/filterAB.py,../abtest/abUser.txt,infoFilter.sh,methodInfo.py,infoReduce.py,infoReduce.sh,../column.py \
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
		-output $outputDir \
		-mapper "sh infoFilter.sh" \
		-reducer "sh infoReduce.sh"

		hdfs dfs -cat $outputDir/* > backup.0804/methodInfoAB.$today.B

	done

