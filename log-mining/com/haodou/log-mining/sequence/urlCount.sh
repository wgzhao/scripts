
for((i=1;i<150;i++))
	do
		day=`date -d -"$i"day +%Y-%m-%d`

		echo $day"	"$(hdfs dfs -text /user/yarn/logs/source-log.http.m_haodou_com/logdate=$day/* | python urlCount.py) >> ~/data/learn.txt
		
	done

