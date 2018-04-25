
tday=`date -d -1day +%m%d`
mkdir backup.$tday
for((i=1;i<30;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
		outputDir=/user/zhangzhonghui/logcount/follow.count
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files searchHead,followSearch.py,followReduce.py,../column.py,../log/readParaConf.py,../log/dictInfo.py,../log/ccinfo.py \
		-D mapred.map.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=10 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D stream.recordreader.compression=gzip \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python followSearch.py searchHead search.getlist@offset:0@return_request_id:boolfalse offset" \
		-reducer "python followReduce.py"

		hdfs dfs -cat $outputDir/* > backup.$tday/followSearch.$today

	done

