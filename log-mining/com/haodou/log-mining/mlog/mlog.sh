tday=`date -d -1day +%m%d`
mkdir backup.$tday

for((i=1;i<90;i++))
	do
		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/user/yarn/logs/source-log.http.m_haodou_com/$today
		outputDir=/user/zhangzhonghui/logcount/mlog.count
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ../log/dictInfo.py,../log/ccinfo.py,mlog.py,../log/infoReduce2.py \
		-D mapred.map.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=10 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D stream.recordreader.compression=gzip \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python mlog.py" \
		-reducer "python infoReduce2.py"

		hdfs dfs -cat $outputDir/* > backup.$tday/mlog.user.$today

	done

