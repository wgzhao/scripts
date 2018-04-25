function count(){
	hdfs dfs -text /user/yarn/logs/source-log.php.CDA39907/$1/* | python regCount.py > ~/data/reg/regCount.$1
}
count 2014-07-14
count 2014-07-15
count 2014-07-16
count 2014-07-17
count 2014-07-18
count 2014-07-19
count 2014-07-20


