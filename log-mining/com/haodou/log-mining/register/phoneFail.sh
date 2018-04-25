

		inputDir=/user/zhangzhonghui/logcount/userSort/*/p*
		outputDir=/user/zhangzhonghui/logcount/reg/phoneFail
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files phoneFail.py,regSeq.py,regMethod.txt,regUserMethod.txt,../column.py,../util/columnUtil.py,../abtest/column2.py  \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=1 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python phoneFail.py map" \
		-reducer "python phoneFail.py reduce"

		hdfs dfs -text $outputDir/* > ~/data/phone.txt

		#hdfs dfs -rm -r $outputDir

