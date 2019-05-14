source ./conf.sh

function add(){
addDir=$1
lastDir=$2
todayDir=$3

#增量
hadoop fs -rm -r $addDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files add.py,$data_tmp_dir/quality.df \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=50 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input $lastDir \
	-input $todayDir \
	-output $addDir \
	-mapper "cat" \
	-reducer "python add.py"
}


function filterAdd(){
addDir=$1
todayAddDir=$2
sumThre=$3

hadoop fs -rm -r $todayAddDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files filter.py \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=0 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input $addDir \
	-output $todayAddDir \
	-mapper "python filter.py $sumThre"

}


function filter(){
addDir=$1
allFilterDir=$2
sumThre=$3

if [ -z $sumThre ]; then
	sumThre=10.0
fi


hadoop fs -rm -r $allFilterDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files filter.py \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=0 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input $addDir \
	-output $allFilterDir \
	-mapper "python filter.py $sumThre true"

}

