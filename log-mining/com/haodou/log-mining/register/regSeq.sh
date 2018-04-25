
		
		inputDir=/user/zhangzhonghui/logcount/sequence/2014-12-*2/*
		outputDir=/user/zhangzhonghui/logcount/reg/regSeq
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files regSeq.py,regMethod.txt,regUserMethod.txt \
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
		-mapper "python regSeq.py map" \
		-reducer "python regSeq.py reduce"

		hdfs dfs -text $outputDir/* > ~/data/regSeq.txt

		#hdfs dfs -rm -r $outputDir

