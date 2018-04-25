i=1
today=`date -d -"$i"day +%Y-%m-%d`

echo "today: "$today

outDir=/user/zhangzhonghui/tagRecipe/$today
todayDir=/user/zhangzhonghui/tagRecipe/today

inputDir=/backup/CDA39907/001/$today
while [ -z $e ]; #将中间缺少的天数全部累加
do
	i=`expr $i + 1`
	echo "i:"$i
	yesterday=`date -d -"$i"day +%Y-%m-%d`
	echo "yesterday: "$yesterday
	lastDir=/user/zhangzhonghui/tagRecipe/$yesterday
	e=$(hdfs dfs -count $lastDir | awk '{print $1}')

	echo "e: "$e

	if [ -z $e ]; then
		inputDir=$inputDir",/backup/CDA39907/001/"$yesterday
		echo $inputDir
	##如果过去没有记录，则重新全部计算。
		if [ $i -gt 30 ]; then
			echo "all!!"
			sh tagRecipe.all.sh; 
			exit 0; 
		fi
	fi

done

echo $lastDir
echo $inputDir
echo $outDir

hadoop fs -rm -r $todayDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files quality.rid,tagRecipe.py,column.py,boundedQueue.py,coReduce.py \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=5 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-input  $inputDir \
	-output $todayDir \
	-mapper "python tagRecipe.py" \
	-reducer "python coReduce.py" 

#增量
hadoop fs -rm -r $outDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files coReduce.py \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=20 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-input $lastDir \
	-input $todayDir \
	-output $outDir \
	-mapper "cat" \
	-reducer "python coReduce.py"

#删除10天前的数据，避免数据量过分膨胀
lastDay=`date -d -10day +%Y-%m-%d`
hadoop fs -rm -r /user/zhangzhonghui/tagRecipe/$lastDay

