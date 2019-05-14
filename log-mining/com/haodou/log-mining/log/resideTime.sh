
tday=`date -d -1day +%m%d`
mkdir ~/data/reside

for((i=1;i<10;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		echo $today

		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		inputDir=/backup/CDA39907/001/$today
		outputDir=/user/zhangzhonghui/logcount/resideTime
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files resideMap.py,resideReduce.py,../column.py,dictInfo.py,ccinfo.py \
		-D mapred.map.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=10 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python resideMap.py" \
		-reducer "python resideReduce.py"

		hdfs dfs -text $outputDir/* > ~/data/reside/residen.$today
		
	done

python resideMerge.py ~/data/reside > ~/data/reside/all.reside



