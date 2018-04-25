source ../conf.sh

function count(){
AB=$1
if [ -z $AB ]; then
	echo "not appoint AB option!!"
	exit 1
fi
echo "AB:"$AB

		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=$(python pathFind.py request)
		
		if [ -z $inputDir ]; then
			echo "empty inputDir!!"
			exit 1;
		fi

		echo $inputDir

		outputDir=$root_hdfs_dir/logcount/methodInfoAB
		
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ../log/ccinfo.py,../log/dictInfo.py,filterAB.py,abUser.txt,infoFilter.sh,../log/methodInfo.py,../log/infoReduce.py,../log/infoReduce.sh,../column.py \
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
		-mapper "sh infoFilter.sh $AB" \
		-reducer "sh infoReduce.sh"


}

while read myline
do
	count $myline
	hdfs dfs -text $outputDir/* > ~/data/backup.AB/methodInfoAB.$myline
done < ABOption.txt


