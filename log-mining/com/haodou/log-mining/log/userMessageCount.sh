inputDir=/user/zhangzhonghui/logcount/userMessage
outputDir=/user/zhangzhonghui/logcount/userMessageCount

hadoop fs -rm -r $outputDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-files userMessageCount.py \
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
-mapper "cat" \
-reducer "python userMessageCount.py"


