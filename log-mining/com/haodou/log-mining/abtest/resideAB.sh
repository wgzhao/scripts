source ../conf.sh

function count(){
AB="A"
if [ ! -z $1 ]; then
	AB=$1
fi
echo "AB:"$AB
today=`date +%Y-%m-%d`

		inputDir=$(python pathFind.py request)
		if [ -z $inputDir ]; then
			echo "empty inputDir!!"
			exit 1
		fi

		outputDir=$root_hdfs_dir/logcount/resideAB
		
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files ../log/dictInfo.py,../log/ccinfo.py,filterAB.py,abUser.txt,resideFilter.sh,../column.py,../log/resideMap.py,../log/resideReduce.py \
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
		-mapper "sh resideFilter.sh $AB" \
		-reducer "python resideReduce.py"

		hdfs dfs -text $outputDir/* > ~/data/backup.AB/resideAB.$AB

}

while read myline
	do
	count $myline
done < ABOption.txt

#python ../log/resideMerge.py ~/data/backup.AB/ resideAB.

