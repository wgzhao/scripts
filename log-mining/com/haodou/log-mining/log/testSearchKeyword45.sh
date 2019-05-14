source ../conf.sh

source ./joinMultiQP.sh
source ./groupSort.sh 
source ./searchKeyword45.sh

s=1
if [ ! -z $1 ];then
	s=$1
fi
e=2
if [ ! -z $2 ];then
	e=$2
fi
div=1
if [ ! -z $3 ]; then
	div=$3
fi
for((i=$s;i<$e;i+=1))
do
	today=`date -d -"$i"day +%Y-%m-%d`
	join $today     $root_hdfs_dir/logcount/joinQP/
	groupsort $root_hdfs_dir/logcount/joinQP/ $root_hdfs_dir/logcount/userSort/$today
	s45 $today
	hdfs dfs -rm -r $root_hdfs_dir/logcount/userSort/$today
done

