
function queryTag(){
if [ -z $2 ]; then
	echo "queryTag: 没有指定起止日期！"
	exit 1
fi
s=$1
e=$2
for((i=$s;i<$e;i++))
	do
		queryTagToday=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=/backup/CDA39907/001/$queryTagToday/*
		outputDir=/user/zhangzhonghui/logcount/queryTag/$queryTagToday
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files partCates.txt,segDict.txt,../tag/code0327/mergeCateName.txt,needWords.txt,queryTag.py,../column.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=0 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python queryTag.py"


	done
}

#queryTag

function tagReduce(){
inputDir=/user/zhangzhonghui/logcount/queryTag/2014-08-1*/*,/user/zhangzhonghui/logcount/queryTag/2014-08-0*/*,/user/zhangzhonghui/logcount/queryTag/2014-07*/*
inputDir=/user/zhangzhonghui/logcount/queryTag/*
outputDir=/user/zhangzhonghui/logcount/tagReduce

hadoop fs -rm -r $outputDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-files tagReduce.py \
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
-reducer "python tagReduce.py 3 0.01"

}

#tagReduce

