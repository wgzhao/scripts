
inputDir=$(python pathFind.py response_user)
if [ -z $inputDir ]; then
	echo "path find fail!";
	exit 1 
fi
echo $inputDir

responseDir=/user/zhangzhonghui/logcount/response

hadoop fs -rm -r $responseDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	 -files filterResp.py,column2.py,ABOption.txt \
	 -D mapred.map.tasks=10 \
	 -D mapred.job.map.capacity=30 \
	 -D mapred.map.overcapacity.allowed=false \
	 -D mapreduce.job.queuename=default \
	 -D  mapred.reduce.tasks=0 \
	 -D mapreduce.map.memory.mb=8192 \
	 -D stream.recordreader.compression=gzip \
	 -input $inputDir \
	 -output $responseDir \
	 -mapper "python filterResp.py"


