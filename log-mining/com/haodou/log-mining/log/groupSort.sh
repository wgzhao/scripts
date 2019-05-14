function groupsort(){
		
		inputDir=$1
		outputDir=$2
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-D stream.num.map.output.key.fields=2 \
		-D num.key.fields.for.partition=1 \
		-D	mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D mapred.output.compress=true \
		-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-input  $inputDir \
		-output $outputDir \
		-mapper "cat" \
		-reducer "cat"

	
}

#groupsort /user/zhangzhonghui/logcount/joinQP/part-00002 /user/zhangzhonghui/logcount/tmp

