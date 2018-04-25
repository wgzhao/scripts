source ./conf.sh

i=1
today=`date -d -"$i"day +%Y-%m-%d`

echo "today: "$today

addDir=/user/zhangzhonghui/tagRecipe/$today
todayDir=/user/zhangzhonghui/tagRecipe/today
todayAddDir=/user/zhangzhonghui/tagRecipe/today.add
allFilterDir=/user/zhangzhonghui/tagRecipe/today.filter

#inputDir=/user/yarn/logs/source-log.php.CDA39907/$today
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
echo $addDir
echo $todayAddDir

function today(){
todayDir=$1
inputDir=$2

hadoop fs -rm -r $todayDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files util/FileUtil.py,$data_tmp_dir/quality.rid,tagRecipe.py,column.py,boundedQueue.py \
	-D mapred.map.tasks=10 \
	-D mapred.job.map.capacity=30 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=0 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input  $inputDir \
	-output $todayDir \
	-mapper "python tagRecipe.py"
}

#计算近期尚没有累加到历史数据中的数据，通常是当天数据
today $todayDir $inputDir

source ./add.sh

add $addDir $lastDir $todayDir

#过滤数据，去掉低频tag
filterAdd $addDir $todayAddDir 20.0
filter $addDir $allFilterDir 20.0

source ./tagid.sh
oldDictDir=/user/zhangzhonghui/tagid/old
dictDir=/user/zhangzhonghui/tagid/dict

hdfs dfs -mkdir -p $oldDictDir

#为尚未存储到字典中的tag赋予id
tag2idDict $allFilterDir $oldDictDir $dictDir
hdfs dfs -rm -r $oldDictDir
hdfs dfs -cp $dictDir $oldDictDir

#将增量数据中的tag转化成对应的id
tag2idData $todayAddDir $dictDir $todayAddDir.id
#将累计数据中的tag转化成对应的id
tag2idData $allFilterDir $dictDir $allFilterDir.id


#删除5天前的数据，避免数据量过分膨胀
lastDay=`date -d -5day +%Y-%m-%d`
hdfs dfs -rm -r /user/zhangzhonghui/tagRecipe/$lastDay


