source pathFind.sh

function join(){
		today=$1

		inputDir=/user/yarn/logs/source-log.php.CDA39907/$today/
		inputDir=$inputDir","/user/yarn/logs/source-log.php.CDA39907.resp/$today/
		inputDir=$inputDir","/user/yarn/logs/source-log.http.m_haodou_com/$today/
		
		echo $inputDir

		outputDir=$2

		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files joinQP.py,../column.py,../mlog/mlog.py,../abtest/column2.py,../util/columnUtil.py \
		-D mapreduce.job.queuename=default \
		-D  mapred.reduce.tasks=30 \
		-D mapred.job.reduce.capacity=30 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python joinQP.py qid" \
		-reducer "python joinQP.py"
}

#today=`date -d -1day +%Y-%m-%d`
#join $today /user/zhangzhonghui/logcount/tmp

