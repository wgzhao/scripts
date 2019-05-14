if [ -z $1 ]; then
	echo "match.sh 未指定日期!!"
fi


		today=$1

		inputDir=/user/zhangzhonghui/logcount/push/filterUserMid,/user/zhangzhonghui/logcount/queryTag/2014-12-1*/*
		outputDir=/user/zhangzhonghui/logcount/push/userTags/$today
		
		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files conf.py,uuid.py,../column.py,../log/queryTag.py,../util/DictUtil.py,weitTag.py,needWords.txt,mergeCateName.txt,segDict.txt,partCates.txt,match.py,itemBank.py,itemBank.txt,defaultPushItem.txt,itemRate.random.txt,idf.py,df.txt  \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.map.overcapacity.allowed=false \
		-D mapreduce.job.queuename=default \
		-D mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python uuid.py" \
		-reducer "python match.py $today"



