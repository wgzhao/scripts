



	
	outputDirMid=/user/zhangzhonghui/logcount/userMessage.mid
	outputDir=/user/zhangzhonghui/logcount/messageDiv
	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files messageDiv.py,dictInfo.py,ccinfo.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=5 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $outputDirMid \
		-output $outputDir \
		-mapper "cat" \
		-reducer "python messageDiv.py"


