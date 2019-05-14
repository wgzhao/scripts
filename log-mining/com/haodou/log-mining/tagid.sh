
function tag2idDict(){
addDir=$1
oldDictDir=$2
newDictDir=$3

#reduce任务只能是1个，多个的话id没法做到唯一性
hadoop fs -rm -r $newDictDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files tagid.py \
	-D mapred.map.tasks=10 \
	-D mapred.job.map.capacity=30 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=1 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input $addDir \
	-input $oldDictDir \
	-output $newDictDir \
	-mapper "python tagid.py listTags" \
	-reducer "python tagid.py dict"
}

function tag2idData(){
addDir=$1
dictDir=$2
idDataDir=$3

hdfs dfs -rm -r $idDataDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files tagid.py \
	-D mapred.map.tasks=10 \
	-D mapred.job.map.capacity=30 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=20 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input $addDir \
	-input $dictDir \
	-output $idDataDir \
	-mapper "cat" \
	-reducer "python tagid.py data"

}

function exchangeData(){
dataDir=$1
dictDir=$2
idDataDir=$3

hdfs dfs -rm -r $idDataDir.tmp
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files tagid.py \
	-D mapred.map.tasks=10 \
	-D mapred.job.map.capacity=30 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=20 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input $dataDir \
	-input $dictDir \
	-output $idDataDir.tmp \
	-mapper "python tagid.py exchange" \
	-reducer "python tagid.py data"

#重新按用户排序
hdfs dfs -rm -r $idDataDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-files tagid.py \
-D mapred.map.tasks=10 \
-D mapred.job.map.capacity=30 \
-D mapred.map.overcapacity.allowed=false \
-D mapreduce.job.queuename=default \
-D  mapred.reduce.tasks=20 \
-D mapreduce.map.memory.mb=8192 \
-D mapreduce.reduce.memory.mb=8192 \
-input $idDataDir.tmp \
-output $idDataDir \
-mapper "python tagid.py exchange" \
-reducer "cat" 

hdfs dfs -rm -r $idDataDir.tmp

}

