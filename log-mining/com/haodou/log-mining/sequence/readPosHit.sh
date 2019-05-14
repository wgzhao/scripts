
mkdir ~/data/wap

s=1
e=4
files=""
names=""

for((i=$s;i<=$e;i++))
	do
		day=`date -d -"$i"day +%Y-%m-%d`

		if [ $i -gt $s ]; then
			files=$files","
			names=$names","
		fi

		hdfs dfs -text /user/zhangzhonghui/logcount/posHit/$day/p* | python readPosHit.py wap > ~/data/wap/wap.$day 2> ~/data/wap/err.$day.txt
        files=$files"/home/zhangzhonghui/data/wap/wap."$day
		names=$names"wap."$day"详表"
		hdfs dfs -text /user/zhangzhonghui/logcount/posHit/$day/p* | python readPosHit.py t2 > ~/data/wap/t2.$day 2> ~/data/wap/t2.err.$day.txt
		files=$files",/home/zhangzhonghui/data/wap/t2."$day
		names=$names",旧版tag."$day"详表"

	done

start=`date -d -"$e"day +%Y-%m-%d`
end=`date -d -"$s"day +%Y-%m-%d`
python ../util/txt2excel.py $files /home/zhangzhonghui/data/wap/热门标签wap页统计.$start"-"$end.xls $names

