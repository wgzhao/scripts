
tday=`date -d -1day +%m%d`
mkdir backup.$tday
for((i=1;i<2;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		outputDir=/user/zhangzhonghui/logcount/follow.count
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files likehead,followMethod.py,followReduce.py,../column.py,../log/readParaConf.py,../log/ccinfo.py,../log/dictInfo.py \
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
		-mapper "python followMethod.py likehead like.add id" \
		-reducer "python followReduce.py"

		hdfs dfs -cat $outputDir/* > backup.$tday/followInfoLike.$today

	done

