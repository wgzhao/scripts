
day=$1
mid=$2
title=$3

day=2015-03-25
mid=5353124
title=炒面的26种不败做法

day=2015-04-23 
mid=6520387
title=菠萝

#hdfs dfs -text /online_logs/beijing/behaviour/pushview/$day/* /backup/CDA39907/001/$day/* /user/yarn/logs/source-log.http.m_haodou_com/logdate=$day/* | python checkPushView.py $mid $title

hdfs dfs -text /user/yarn/logs/source-log.http.m_haodou_com/logdate=$day/* /backup/CDA39907/001/$day/* /online_logs/beijing/behaviour/pushview/$day/* | python checkPushView.py $mid $title

#hdfs dfs -text /backup/CDA39907/001/2015-03-03/* /online_logs/beijing/behaviour/pushview/2015-03-03/* | python checkPushView.py 5993308 分钟搞定快手早餐，多睡半小时 | more

