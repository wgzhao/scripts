function reserve(){
		
		#inputDir=/user/yarn/logs/source-log.php.CDA39907/2014-12-10/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-11/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-12/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-13/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-14/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-15/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-16/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-17/*,/user/yarn/logs/source-log.php.CDA39907/2014-12-18/*
		inputDir=$(python ../abtest/pathJoin.py request $2 $3)
		
		outputDir=/user/zhangzhonghui/reserve
		toDo=$1
		touch user.txt
		
		echo $inputDir

		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files userReserve.py,user.txt,../column.py \
		-D	mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=5 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D mapred.output.compress=true \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python userReserve.py "$toDo \
		-reducer "python userReserve.py reduce"

	
}

reserve getUser 2014-10-01 2014-10-03
 hdfs dfs -text /user/zhangzhonghui/reserve/p* > user.txt
reserve reserve 2014-10-04 2014-10-07
hdfs dfs -text /user/zhangzhonghui/reserve/p* | python userReserve.py rate

