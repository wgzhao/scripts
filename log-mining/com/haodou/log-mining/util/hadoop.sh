source ../conf.sh

root_dir=$root_local_dir

function basic(){

		inputDir=$1
		outputDir=$2
		files=$3
		if [ ! -z $6 ]; then
			root_dir=$6
		fi
		mapper=$(python $root_dir/util/hadoopUtil.py split $4)
		reducer=$(python $root_dir/util/hadoopUtil.py split $5)
		
		if [ $4 == "python" ]; then
			echo "wrong mapper expression!"
			exit 1
		fi
		
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files  $files \
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
		-mapper "$mapper" \
		-reducer "$reducer"

}

function basicMultiOutput(){
	inputDir=$1
	outputDir=$2
	files=$3

	if [ ! -z $6 ]; then
		root_dir=$6
	fi

	mapper=$(python $root_dir/util/hadoopUtil.py split $4)
	reducer=$(python $root_dir/util/hadoopUtil.py split $5)
	if [ $4 == "python" ]; then
		echo "wrong mapper expression!"
		exit 1
	fi

	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files  $files \
	-D stream.num.map.output.key.fields=1 \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapred.job.reduce.capacity=50 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-outputformat org.apache.hadoop.mapred.lib.SuffixMultipleTextOutputFormat \
	-input  $inputDir \
	-output $outputDir \
	-mapper "$mapper" \
	-reducer "$reducer"
}

function basicNoReduce(){
	inputDir=$1
	outputDir=$2
	files=$3

	if [ ! -z $5 ]; then
		root_dir=$5
	fi

	mapper=$(python $root_dir/util/hadoopUtil.py split $4)

	if [ $4 == "python" ]; then
		echo "wrong mapper expression!"
		exit 1
	fi

	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files  $files \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=0 \
	-D mapreduce.map.memory.mb=8192 \
	-input  $inputDir \
	-output $outputDir \
	-mapper "$mapper" 

}

function basicLZO(){
	inputDir=$1
	outputDir=$2
	files=$3

	if [ ! -z $6 ]; then
		root_dir=$6
	fi

	mapper=$(python $root_dir/util/hadoopUtil.py split $4)
	reducer=$(python $root_dir/util/hadoopUtil.py split $5)

	if [ $4 == "python" ]; then
		echo "wrong mapper expression!"
		exit 1
	fi

	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files  $files \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapred.job.reduce.capacity=50 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D stream.map.input.ignoreKey=true \
	-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat \
	-input  $inputDir \
	-output $outputDir \
	-mapper "$mapper" \
	-reducer "$reducer"



}


function groupsort(){
		
		inputDir=$1
		outputDir=$2
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-D stream.num.map.output.key.fields=2 \
		-D num.key.fields.for.partition=1 \
		-D	mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-D mapred.output.compress=true \
		-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
		-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
		-input  $inputDir \
		-output $outputDir \
		-mapper "cat" \
		-reducer "cat"

	
}


function mrGroupsortLZO(){
	inputDir=$1
	outputDir=$2
	files=$3

	if [ ! -z $6 ]; then
		root_dir=$6
	fi

	mapper=$(python $root_dir/util/hadoopUtil.py split $4)
	reducer=$(python $root_dir/util/hadoopUtil.py split $5)

	if [ $4 == "python" ]; then
		echo "wrong mapper expression!"
		exit 1
	fi

	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files $files \
	-D stream.num.map.output.key.fields=2 \
	-D num.key.fields.for.partition=1 \
	-D  mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapred.job.reduce.capacity=50 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D mapred.output.compress=true \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-D stream.map.input.ignoreKey=true \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
	-inputformat com.hadoop.mapred.DeprecatedLzoTextInputFormat	\
	-input  $inputDir \
	-output $outputDir \
	-mapper "$mapper" \
	-reducer "$reducer"

}




function mrGroupsort(){
	inputDir=$1
	outputDir=$2
	files=$3

	if [ ! -z $6 ]; then
		root_dir=$6
	fi

	mapper=$(python $root_dir/util/hadoopUtil.py split $4)
	reducer=$(python $root_dir/util/hadoopUtil.py split $5)

	if [ $4 == "python" ]; then
		echo "wrong mapper expression!"
		exit 1
	fi

	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files $files \
	-D stream.num.map.output.key.fields=2 \
	-D num.key.fields.for.partition=1 \
	-D  mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapred.job.reduce.capacity=50 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D mapred.output.compress=true \
	-D mapred.output.compression.codec=com.hadoop.compression.lzo.LzopCodec \
	-partitioner org.apache.hadoop.mapred.lib.KeyFieldBasedPartitioner \
	-input  $inputDir \
	-output $outputDir \
	-mapper "$mapper" \
	-reducer "$reducer"

}


