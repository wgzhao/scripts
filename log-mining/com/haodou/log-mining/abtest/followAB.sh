
source ../conf.sh

function count(){
AB="A"
if [ ! -z $1 ]; then
	AB=$1
fi
echo "AB:"$AB

followType="info"
if [ ! -z $2 ]; then
	followType=$2
fi
echo "followType:"$followType

		today=`date -d -"$i"day +%Y-%m-%d`
		
		inputDir=$(python pathFind.py request)
	
		if [ -z $inputDir ]; then
			echo "empty inputDir!!"
			exit 1
		fi

		echo $inputDir

		outputDir=$root_hdfs_dir/logcount/followAB
		
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files likehead,f1,followMethod.py,followReduce.py,../log/dictInfo.py,../log/ccinfo.py,filterAB.py,abUser.txt,filterFollow.sh,../column.py,../log/readParaConf.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=30 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "sh filterFollow.sh $AB $followType" \
		-reducer "python followReduce.py"

}


while read myline
do
	count $myline
	hdfs dfs -text $outputDir/* > ~/data/backup.AB/followAB.$myline

	#count $myline like
	#hdfs dfs -text $outputDir/* > backup.AB.$tday/followLikeAB.$myline

done < ABOption.txt

