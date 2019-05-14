source ../conf.sh

function s45(){

		today=$1
		inputDir=$root_hdfs_dir/logcount/userSort/$today
		outputDir=$root_hdfs_dir/logcount/searchKeyword45/$today
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ../util/columnUtil.py,./moreSearchCount.py,./cardClick.py,../util/DictUtil.py,searchKeyword45.py,../column.py,../util/columnUtil.py,../abtest/column2.py,clickCount.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=10 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python searchKeyword45.py map" \
		-reducer "python searchKeyword45.py reduce"


}
#s45 $1


