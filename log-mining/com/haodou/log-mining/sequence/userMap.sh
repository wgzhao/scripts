
for((i=15;i< 35;i+=1))
	do
		
		day=`date -d -"$i"day +%Y-%m-%d`
		echo $day"	"$(hdfs dfs -text /backup/CDA39907/001/$day/* | python userMap.py)

	done
