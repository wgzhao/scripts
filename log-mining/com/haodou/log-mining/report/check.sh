function check(){
	echo $1"	"$(hdfs dfs -text /user/zhangzhonghui/logcount/searchKeyword45/$1/p* 2>/dev/null | python check.py | grep -v total | grep -v topic)

	#echo $1"	"$(hdfs dfs -text /user/zhangzhonghui/logcount/searchKeyword45/$1/p* 2> /dev/null | python tagQuery.py)

}

check 2015-01-21

