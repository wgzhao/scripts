source ../conf.sh
source sequence.sh

for((i=1;i<2;i+=1))
	do
	if [ $i -gt 20 ]; then
		i=`expr $i + 1`	
	fi
	today=`date -d -"$i"day +%Y-%m-%d`
	a=$(hdfs dfs -count $root_hdfs_dir/logcount/userSort/$today | awk '{print $1}')
	
	if [ -z $a ]; then
		cd $root_local_dir/log
		source joinMultiQP.sh
		source groupSort.sh
		join $today     $root_hdfs_dir/logcount/joinQP/
		groupsort $root_hdfs_dir/logcount/joinQP/ $root_hdfs_dir/logcount/userSort/$today
		cd $root_local_dir/sequence

		seq $today
		hdfs dfs -rm -r $root_hdfs_dir/logcount/userSort/$today
	else
		seq $today
	fi

	sh seqHit.sh $today
	sh posHit.sh $today
	
	hdfs dfs -rm -r $root_hdfs_dir/logcount/sequence/$today

done


