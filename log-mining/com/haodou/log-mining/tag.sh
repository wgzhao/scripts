t1=`date -d -1day +%Y-%m`
t2=`date -d -1month +%Y-%m`
t3=`date -d -2month +%Y-%m`


outDir=/tmp/tagRecipe/


hadoop fs -rm -R -skipTrash$outDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
-files quality.rid,tagRecipe.py,column.py,boundedQueue.py,coReduce.py\
-D mapred.job.map.capacity=50 \
-D mapred.map.tasks=50 \
-D mapred.map.overcapacity.allowed=false\
-D mapreduce.job.queuename=default \
-D mapred.reduce.tasks=10 \
-D mapreduce.map.memory.mb=8192 \
-D mapreduce.reduce.memory.mb=8192 \
-input /backup/CDA39907/001/$t1*/* \
-input /backup/CDA39907/001/$t2*/* \
-input /backup/CDA39907/001/$t3*/* \
-output $outDir \
-mapper "python tagRecipe.py" \
-reducer "python coReduce.py"


