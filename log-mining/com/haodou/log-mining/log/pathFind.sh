
function check(){
	hdfs dfs -count $1/*.lzo > /dev/null

	if  [ $? -ne 0 ] ;then
		echo $1
		return
	fi
	echo $1/*.lzo

}

