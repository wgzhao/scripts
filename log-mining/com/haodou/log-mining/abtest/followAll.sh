#统计若干天中的情况
#

start=2014-08-01
end=2014-08-31

tday=`date -d -1day +%m%d`
mkdir backup.$tday

		inputDir=$(python pathJoin.py request $start $end)
		outputDir=/user/zhangzhonghui/logcount/follow.count
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files f1,followMethod.py,followReduceAll.py,../column.py,../log/readParaConf.py,../log/dictInfo.py,../log/ccinfo.py \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D stream.recordreader.compression=gzip \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python followMethod.py f1 info.getinfo#info.getlastestinfo rid" \
		-reducer "python followReduceAll.py"

		hdfs dfs -cat $outputDir/* > backup.$tday/followInfo.$start.$end


