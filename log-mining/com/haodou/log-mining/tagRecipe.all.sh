source ./conf.sh

today=`date -d -2day +%Y-%m-%d`


outDir=/user/zhangzhonghui/tagRecipe/$today


hadoop fs -rm -R -skipTrash $outDir
hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files util/FileUtil.py,tagRecipe.py,$data_tmp_dir/quality.rid,$data_tmp_dir/quality.df,add.py,column.py,boundedQueue.py \
	-D mapred.map.tasks=100 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=100 \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-D stream.recordreader.compression=gzip \
	-output $outDir \
	-input  /backup/CDA39907/001/*/* \
	-output $outDir \
	-mapper "python tagRecipe.py" \
	-reducer "python add.py"



