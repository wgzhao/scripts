
tday=`date -d -1day +%m%d`
mkdir backup.$tday
for((i=1;i<7;i+=1))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		outputDir=/user/zhangzhonghui/logcount/follow.count
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files f1,followMethod.py,followReduce2.py,../column.py,../log/readParaConf.py,../log/dictInfo.py,../log/ccinfo.py \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D stream.recordreader.compression=gzip \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python followMethod.py f1 info.getinfo#info.getlastestinfo rid" \
		-reducer "python followReduce2.py"

		hdfs dfs -cat $outputDir/* > backup.$tday/followInfo.$today

	done

