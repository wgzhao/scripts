function merge(){
		
		inputDir=/user/zhangzhonghui/logcount/sequence/2014-1*
		outputDir=/user/zhangzhonghui/logcount/loss.merge
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files mergeData.py,user.txt \
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
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python mergeData.py" \
		-reducer "cat"

	
}

#merge

function lossAnalysis(){
	inputDir=/user/zhangzhonghui/logcount/loss.merge
	outputDir=/user/zhangzhonghui/logcount/loss
	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files  mergeData.py,user.txt,analysis.py,readHitMethod.py,hitMethod.txt,../column.py,../util/TimeUtil.py,../util/DictUtil.py,../sequence/actionUserInfo.py \
	-D  mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=0 \
	-D mapreduce.map.memory.mb=8192 \
	-input  $inputDir \
	-output $outputDir \
	-mapper "python analysis.py"
}

lossAnalysis



