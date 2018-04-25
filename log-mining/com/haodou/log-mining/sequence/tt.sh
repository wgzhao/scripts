function t1(){
	source ../conf.sh
	a=$(hdfs dfs -count $root_hdfs_dir/logcount/userSort/2014-12-19 | awk '{print $1}')
	if [ -z $a ]; then
		echo "a "$a
		echo "haha"
	fi
}

function t2(){

	for((i=16;i<35;i+=1))
		do
			day=`date -d -"$i"day +%Y-%m-%d`
	#hdfs dfs -text /user/zhangzhonghui/logcount/posHit/$day/* | awk -F"	" '{if($3 > 5) print $0}' | python readPosHit.py search.getlist_k1 | grep -v search.getcatelist | grep -v t1 | grep -v t2 | grep -v search.getsuggestion | grep -v rid | grep -v aid | grep -v food | awk -v day=$day '{OFS="\t";print day,$0}' >> ~/data/tt0207
	echo $day"	"$(hdfs dfs -text /user/zhangzhonghui/logcount/posHit/$day/* | awk -F"	" '{if($3 > 3) print $0}'  | python readPosHit.py search.getsearchindex_k1)

	done

}

t2

