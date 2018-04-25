#source pathFind.sh

function join(){
		today=$1
		
		if [[ $today < '2015-01-01' ]];then

			inputDir=/user/yarn/logs/source-log.php.CDA39907/$today/
			inputDir=$inputDir","/user/yarn/logs/source-log.php.CDA39907.resp/$today/
		else
			inputDir=/backup/CDA39907/001/$today/
			inputDir=$inputDir","/backup/CDA39907/002/$today/
		fi
		
		inputDir=$inputDir","/user/yarn/logs/source-log.http.m_haodou_com/logdate=$today/
		
		echo $inputDir

		outputDir=$2

		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files joinMultiQP.py,../column.py,../mlog/mlog.py,../abtest/column2.py,../util/columnUtil.py \
		-D mapreduce.job.queuename=default \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python joinMultiQP.py qid" \
		-reducer "python joinMultiQP.py"
}



function joinBackup(){
		today=$1

		inputDir=/backup/CDA39907/001/$today/
		inputDir=$inputDir","/backup/CDA39907/002/$today/
		inputDir=$inputDir","/user/yarn/logs/source-log.http.m_haodou_com/logdate=$today/
		
		echo $inputDir

		outputDir=$2

		hadoop fs -rm -r $outputDir
		hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
		-files joinMultiQP.py,../column.py,../mlog/mlog.py,../abtest/column2.py,../util/columnUtil.py \
		-D mapreduce.job.queuename=default \
		-D mapred.map.tasks=50 \
		-D mapred.job.map.capacity=50 \
		-D mapred.reduce.tasks=50 \
		-D mapred.job.reduce.capacity=50 \
		-D mapred.reduce.overcapacity.allowed=false \
		-D mapreduce.map.memory.mb=8192 \
		-D mapreduce.reduce.memory.mb=8192 \
		-input  $inputDir \
		-output $outputDir \
		-mapper "python joinMultiQP.py qid" \
		-reducer "python joinMultiQP.py"
}


