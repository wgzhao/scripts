inputDir=/user/zhangzhonghui/userModel.online
inputDir1=/user/zhangzhonghui/userModel/latest

outDir=/user/zhangzhonghui/userRecom

exDir=/user/zhangzhonghui/userRecom.exchange

hadoop fs -rm -r $exDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-input $inputDir \
	-input $inputDir1 \
	-output $exDir \
	-mapper "python exchang12.py" \
	-file exchang12.py \
	-jobconf  mapred.reduce.tasks=0

midDir=/user/zhangzhonghui/userRecom.mid
hadoop fs -rm -r $midDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-input  $exDir \
	-input /user/zhangzhonghui/tagRecipe.online \
	-output $midDir \
	-mapper "cat" \
	-reducer "python userRecom.py" \
	-file userRecom.py  \
	-file CollectionUtil.py \
	-jobconf  mapred.reduce.tasks=20

hadoop fs -rm -r $outDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-input $midDir \
	-output $outDir \
	-mapper "cat" \
	-reducer "python coReduce.py" \
	-file coReduce.py \
	-jobconf  mapred.reduce.tasks=20


