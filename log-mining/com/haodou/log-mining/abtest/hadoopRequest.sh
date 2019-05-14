inputDir=$(python pathFind.py request_user)
if [ -z $inputDir ]; then
	echo "path find fail!";
	exit 1 
fi
echo $inputDir

requestDir=/user/zhangzhonghui/logcount/request

hadoop fs -rm -r $requestDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	 -files ../column.py,filterReq.py \
	 -D mapred.map.tasks=10 \
	 -D mapred.job.map.capacity=30 \
	 -D mapred.map.overcapacity.allowed=false \
	 -D mapreduce.job.queuename=default \
	 -D  mapred.reduce.tasks=0 \
	 -D mapreduce.map.memory.mb=8192 \
	 -D stream.recordreader.compression=gzip \
	 -input $inputDir \
	 -output $requestDir \
	 -mapper "python filterReq.py"


