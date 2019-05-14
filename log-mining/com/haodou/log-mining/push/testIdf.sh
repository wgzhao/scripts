
hdfs dfs -cat /user/zhangzhonghui/logcount/queryTag/2014-11-29/* | python idf.py map | sort | python idf.py reduce

