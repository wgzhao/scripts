###################################################
N=2
source ../conf.sh
source ../util/hadoop.sh

rootDir=/user/zhangzhonghui/userRecom
if [[ ! -z $1 && $1 == "test" ]]; then
	rootDir=/user/zhangzhonghui/userRecomTest
	echo $rootDir
fi

hdfs dfs -mkdir $rootDir

yesterday=`date -d -1day +%Y-%m-%d`
firstDay=$yesterday   #起始统计日期默认是昨天
AccBaseDir="$rootDir/acc/"
outputAcc=$AccBaseDir"$yesterday"
outputAccTmp=$rootDir/acc.tmp/

hdfs dfs -rm -r $outputAcc

dbActionDir=$rootDir/dbAction/
outputFilter=$rootDir/filter
outputPopular=$rootDir/popular
hdfsFileUtilEcho="import sys\nsys.path.append('../util')\nimport hdfsFile\n"
inputAcc=$dbActionDir/*,$(echo -e $hdfsFileUtilEcho"print hdfsFile.getInputStrPlus('$AccBaseDir',N=$N)" | python )
firstDay=$(echo -e $hdfsFileUtilEcho"print hdfsFile.getLastDay('$AccBaseDir',N=$N)" | python )
###################################################

#11月2日讨论：不再区分视频菜谱和普通菜谱
#python ../readDB/DBItem.py isVideo > $data_tmp_dir/recipe.video.txt  #先获取视频菜谱id

###################################################
#数据库部分行为数据
function dbNut(){
	#会检查到昨天为止，过去的累加数据现在缺哪些数据，只补全那些缺失日期的数据
	python dbNut.py acc $AccBaseDir  $N > $data_tmp_dir/dbActionForUserRecom.txt
	hdfs dfs -mkdir $dbActionDir
	hdfs dfs -rm -r $dbActionDir/*
	hdfs dfs -put $data_tmp_dir/dbActionForUserRecom.txt $dbActionDir
}


###################################################
function acc()
{

	echo -e $hdfsFileUtilEcho"hdfsFile.delDays('$AccBaseDir')" | python
	echo "inputAccDir:"$inputAcc
	echo "outputAccDir:"$outputAccTmp

	source ../util/hadoop.sh

	files=acc.py,../util/TimeUtil.py,../column.py,nut.py,userRecomConf.py,../readDB/ReadDBConf.py,../util/FileUtil.py
	mapper="python,nut.py"
	reducer="python,acc.py"

	basic $inputAcc $outputAccTmp $files $mapper $reducer ../
	
}

if [ $? -ne 0 ]; then 
		echo "生成累加数据失败!";
		exit 1
	fi
###################################################

function invalid()
{
	files=invalid.py
	mapper="python,invalid.py,map"
	reducer="python,invalid.py,reduce"

	python ../readDB/DBItem.py invalidItem > $data_tmp_dir/item.invalid.txt #非法条目列表

	hdfs dfs -mkdir $rootDir/invalid
	hdfs dfs -rm -r $rootDir/invalid/*
	hdfs dfs -put $data_tmp_dir/item.invalid.txt $rootDir/invalid
	
	basic $rootDir/invalid,$outputAccTmp $outputAcc $files $mapper $reducer ../

}


###################################################

function split()
{
	
	if [ $1 == "new" ]; then
		echo $firstDay

		files=../util/TimeUtil.py,userRecomConf.py,outputItem.py
		mapper="cat"
		reducer="python,outputItem.py,latest,$firstDay"
		
		basic $outputAcc $outputFilter $files $mapper $reducer ../
	
		hdfs dfs -text $outputFilter/* | python outputItem.py split
	else
		hdfs dfs -text $outputAcc/* | python outputItem.py split 
	fi
	hdfs dfs -text $outputAcc/* | python outputItem.py split all
}

####################################################

function popular()
{
	files=userRecomConf.py,activeItem.py
	mapper="python activeItem.py num"
	reducer="python activeItem.py filter"
	
	hadoop fs -rm -r $outputPopular
	hadoop jar /usr/hdp/current/hadoop-mapreduce-client/hadoop-streaming.jar \
	-files  $files \
	-D mapred.map.tasks=50 \
	-D mapred.job.map.capacity=50 \
	-D mapred.map.overcapacity.allowed=false \
	-D mapreduce.job.queuename=default \
	-D  mapred.reduce.tasks=1 \
	-D mapred.job.reduce.capacity=1 \
	-D mapred.reduce.overcapacity.allowed=false \
	-D mapreduce.map.memory.mb=8192 \
	-D mapreduce.reduce.memory.mb=8192 \
	-input  $outputAcc \
	-output $outputPopular \
	-mapper "$mapper" \
	-reducer "$reducer"

	if [ $? -ne 0 ]; then
		echo "popular步骤失败!"
	fi

	hdfs dfs -text $outputPopular/* > $data_tmp_dir/item.active.txt
	
	cp $data_tmp_dir/item.active.txt $data_dir/

	python ../readDB/DBItem.py recipeUser > $data_tmp_dir/itemUser.txt
	python ../readDB/DBItem.py photoUser >> $data_tmp_dir/itemUser.txt
	python ../readDB/DBItem.py topicUser >> $data_tmp_dir/itemUser.txt

	cat $data_tmp_dir/itemUser.txt | python userWithActiveItems.py > $data_dir/user.activeItems.txt

	if [ $? -ne 0 ];then
		echo "获取有受欢迎资源的作者列表失败!!"
	else
		cp $data_dir/user.activeItems.txt $data_all_dir/user.activeItems.txt
	fi

}

function follow()
{

	python ../readDB/DBItem.py userFollow > $data_tmp_dir/user.follow.txt
	cat $data_tmp_dir/user.follow.txt | sort | python follow.py > $data_dir/userFollow.txt

  	if [ $? -ne 0 ];then
		echo "获取用户关注列表失败!!"
	else
		cp $data_dir/userFollow.txt $data_all_dir/userFollow.txt
  	fi

}	

####################################################
dbNut

acc

invalid #删除非法条目

#split new 只取最新日期的数据;split all取全部数据
split new

popular

follow #用户关注的豆友


