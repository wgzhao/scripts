source ../conf.sh

function seq(){

		today=$1
		#inputDir=$root_hdfs_dir/logcount/sequence/2014-12-10,$root_hdfs_dir/logcount/sequence/2014-12-15
		inputDir=$root_hdfs_dir/logcount/sequence/2014-12-16,$root_hdfs_dir/logcount/sequence/2014-12-17,$root_hdfs_dir/logcount/sequence/2014-12-18,$root_hdfs_dir/logcount/sequence/2014-12-10,$root_hdfs_dir/logcount/sequence/2014-12-15,$root_hdfs_dir/logcount/sequence/2014-12-11,$root_hdfs_dir/logcount/sequence/2014-12-14,$root_hdfs_dir/logcount/sequence/2014-12-13,$root_hdfs_dir/logcount/sequence/2014-12-12
		outputDir=$root_hdfs_dir/logcount/seqCount/
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files new.user.1210,notSeq.txt,../util/TimeUtil.py,actionInfo.py,actionUserInfo.py,getActionItem.py,../column.py,../util/columnUtil.py,../abtest/column2.py,seqCount.py,date.txt,topNode.txt \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "cat" \
		-reducer "python seqCount.py"


}
seq


