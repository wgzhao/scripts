today=`date -d -1day +%Y-%m-%d`
yesterday=`date -d -2day +%Y-%m-%d`
lastDir=/user/zhangzhonghui/userModel/$yesterday
todayDir=/user/zhangzhonghui/userModel/today
outDir=/user/zhangzhonghui/userModel/$today

echo $lastDir
echo $outDir


hadoop fs -rm -r $todayDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-input  /backup/CDA39907/001/$today/* \
	-output $todayDir \
	-mapper "python userModel.py" \
	-reducer "python coReduce.py" \
	-file userModel.py  \
	-file column.py \
	-file boundedQueue.py \
	-file coReduce.py \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapred.job.reduce.capacity=50 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \

#增量
hadoop fs -rm -r $outDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-input $lastDir \
	-input $todayDir \
	-output $outDir \
	-files coReduce.py \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapred.job.reduce.capacity=50 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat \
	-mapper "cat" \
	-reducer "python coReduce.py"

#删除10天前的数据，避免数据量过分膨胀
lastDay=`date -d -10day +%Y-%m-%d`
hadoop fs -rm -r /user/zhangzhonghui/userModel/$lastDay

