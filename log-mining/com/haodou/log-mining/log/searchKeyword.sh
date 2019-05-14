
tday=`date -d -1day +%m%d`
mkdir backup.$tday

s=1
d=2
sh userSort.sh $s $d


for((i=$s;i<$d;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
	

		inputDir=/user/yanghong/logcount/userSort/$today
		outputDir=/user/yanghong/logcount/searchKeyword
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ../util/columnUtil.py,../mlog/mlog.py,searchKeyword.py,infoReduce.py,infoReduce.sh,../column.py,../abtest/column2.py,dictInfo.py,ccinfo.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=1 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D stream.recordreader.compression=gzip \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python searchKeyword.py Mapper" \
		-reducer "python searchKeyword.py Reduce"

		hdfs dfs -cat $outputDir/* > backup.$tday/searchKeyword.$today

	done


