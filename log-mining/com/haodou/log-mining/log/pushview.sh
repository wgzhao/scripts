
function parse(){
		inputDir=/online_logs/beijing/behaviour/push_received/2014-08*,/online_logs/beijing/behaviour/push_received/2014-07*,/online_logs/beijing/behaviour/pushview/2014-08*,/online_logs/beijing/behaviour/pushview/2014-07*
		outputDirMid=/user/zhangzhonghui/logcount/userMessage.mid
		
		hadoop fs -rm -r $outputDirMid
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files message.txt,pushview.py,../abtest/column2.py,../util/columnUtil.py \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=0 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDirMid \
		-mapper "python pushview.py"
}

parse
	


