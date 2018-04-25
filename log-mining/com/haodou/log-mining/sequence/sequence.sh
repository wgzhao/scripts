source ../conf.sh

function seq(){

		today=$1
		inputDir=$root_hdfs_dir/logcount/userSort/$today
		outputDir=$root_hdfs_dir/logcount/sequence/$today
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files readTagWap.py,tag.wap.txt,nutri.wap.txt,cateidName.txt,sequence.py,actionInfo.py,onlySeq.txt,notSeq.txt,getActionItem.py,actionUserInfo.py,../column.py,../util/columnUtil.py,../abtest/column2.py,../util/DBCateName.py,../util/DictUtil.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=0 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python sequence.py"


}
#seq $1


