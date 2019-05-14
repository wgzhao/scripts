today=$1

source ../conf.sh

source joinMultiQP.sh
source groupSort.sh 
	
join $today	$root_hdfs_dir/logcount/joinQP/
groupsort $root_hdfs_dir/logcount/joinQP/ $root_hdfs_dir/logcount/userSort/$today



