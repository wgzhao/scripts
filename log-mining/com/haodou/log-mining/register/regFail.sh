
source ../conf.sh

		day=2014-09-15

		today=$day
		cd $root_local_dir/log
		source joinMultiQP.sh
		source groupSort.sh
		join $today     $root_hdfs_dir/logcount/joinQP/
		groupsort $root_hdfs_dir/logcount/joinQP/ $root_hdfs_dir/logcount/userSort/$today
		cd ../register

		sh getMaxUserId.sh $day

		ln -s ../loss/hitMethod.txt ./

		inputDir=/user/zhangzhonghui/logcount/userSort/$day/p*
		outputDir=/user/zhangzhonghui/logcount/reg/regFail
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files regFail.py,regSeq.py,regMethod.txt,regUserMethod.txt,../column.py,../util/columnUtil.py,../abtest/column2.py,hitMethod.txt,maxUid.txt  \
		-D stream.num.map.output.key.fields=2 \
		-D num.key.fields.for.partition=1 \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=1 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python regFail.py map" \
		-reducer "python regFail.py reduce"

		hdfs dfs -text $outputDir/* > ~/data/reg/regFail.$day

		#hdfs dfs -rm -r $outputDir

