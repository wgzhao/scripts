
	
	outputDirMid=/user/zhangzhonghui/logcount/userMessage.mid
	outputDir=/user/zhangzhonghui/logcount/userMessage
	hadoop fs -rm -r $outputDir
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files partCates.txt,segDict.txt,mergeCateName.txt,needWords.txt,../abtest/column2.py,../column.py,queryTag.py,userMessageMatch.py,../util/columnUtil.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $outputDirMid \
		-input /user/zhangzhonghui/logcount/tagReduce\
		-output $outputDir \
		-mapper "cat" \
		-reducer "python userMessageMatch.py"


