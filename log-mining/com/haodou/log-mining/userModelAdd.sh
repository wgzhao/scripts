source ./conf.sh

i=1
today=`date -d -"$i"day +%Y-%m-%d`

echo "today: "$today

addDir=/user/zhangzhonghui/userModel/$today
todayDir=/user/zhangzhonghui/userModel/today
todayAddDir=/user/zhangzhonghui/userModel/today.add.id
allFilterDir=/user/zhangzhonghui/userModel/today.filter.id
idAddDir=/user/zhangzhonghui/userModel/$today.id

inputDir=/backup/CDA39907/001/$today
while [ -z $e ]; #将中间缺少的天数全部累加
do
	i=`expr $i + 1`
	echo "i:"$i
	yesterday=`date -d -"$i"day +%Y-%m-%d`
	echo "yesterday: "$yesterday
	lastDir=/user/zhangzhonghui/userModel/$yesterday
	e=$(hdfs dfs -count $lastDir | awk '{print $1}')

	echo "e: "$e

	if [ -z $e ]; then
		inputDir=$inputDir",/backup/CDA39907/001/"$yesterday
		echo $inputDir
	##如果过去没有记录，则重新全部计算。
		if [ $i -gt 30 ]; then
			echo "all!!"
			sh userModel.all.sh; 
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
	-files $data_tmp_dir/quality.rid,userModel.py,column.py,boundedQueue.py \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=0 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-input  $inputDir \
	-output $todayDir \
	-mapper "python userModel.py"
}

today $todayDir $inputDir

source ./add.sh

add $addDir $lastDir $todayDir

source ./tagid.sh

dictDir=/user/zhangzhonghui/tagid/dict

exchangeData $addDir $dictDir $idAddDir

filterAdd $idAddDir $todayAddDir 50.0

filter $idAddDir $allFilterDir 50.0


#删除5天前的数据，避免数据量过分膨胀
lastDay=`date -d -5day +%Y-%m-%d`
hadoop fs -rm -r /user/zhangzhonghui/userModel/$lastDay
hadoop fs -rm -r /user/zhangzhonghui/userModel/$lastDay.id

