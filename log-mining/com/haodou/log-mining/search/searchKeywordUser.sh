source ../conf.sh

function s45(){

		today=$1
		inputDir=$root_hdfs_dir/logcount/userSort/$today
		outputDir=$root_hdfs_dir/logcount/searchKeywordUser/$today.uid
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ../util/columnUtil.py,~/data/uid.1211.txt,../log/moreSearchCount.py,../log/cardClick.py,../util/DictUtil.py,./searchKeywordUser.py,../column.py,../abtest/column2.py,../log/clickCount.py,../util/columnUtil.py \
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
		-mapper "python searchKeywordUser.py map" \
		-reducer "python searchKeywordUser.py reduce"


}
s45 $1


