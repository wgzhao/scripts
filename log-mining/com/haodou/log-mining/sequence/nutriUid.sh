
for((i=0;i< 60;i+=1))
	do
day=

hdfs dfs -text /backup/CDA39907/001/$day/CDA39907*001.AVL.log.tar.lzo | python nutriUid.py ips

hdfs dfs -text /user/yarn/logs/source-log.http.m_haodou_com/logdate=$day/* | python nutriUid.py uidVisit

done

