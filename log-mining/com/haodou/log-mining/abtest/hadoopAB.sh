abDir=/user/zhangzhonghui/logcount/ab

hadoop fs -rm -r $abDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files ../log/dictInfo.py,ab.py \
	-D mapred.map.tasks=10 \
	-D mapred.job.map.capacity=30 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=1 \
	-D mapred.job.reduce.capacity=30 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input /user/zhangzhonghui/logcount/response,/user/zhangzhonghui/logcount/request \
	-output $abDir \
	-mapper "cat" \
	-reducer "python ab.py"


