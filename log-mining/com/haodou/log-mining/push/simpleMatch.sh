
#cat urlSend.txt | python ../log//queryTag.py 千万别给孩子吃植物奶油！最适合宝宝食用的健康小饼干→

today=`date +%Y-%m-%d`

if [ -z $1 ];then
	echo "未指定日期!"
	exit 1
fi

today=$1

mkdir /data/push_tag/$today

lastMonth=$(date +%Y-%m --date="-32 day")
month=$(date +%Y-%m)
echo $lastMonth,$month

#source ../util/hadoop.sh
#groupsort /user/zhangzhonghui/logcount/queryTag/$lastMonth*/*,/user/zhangzhonghui/logcount/queryTag/$month*/* /user/zhangzhonghui/logcount/push/simpleMatch

#hdfs dfs -text /user/zhangzhonghui/logcount/push/simpleMatch/* > /data/push_tag/tmp.simpleMatch.txt

python proUrlSend2.py $1 $2 #新的生成方式

cp w2cs.txt /data/push_tag/$today/

hdfs dfs -text /user/zhangzhonghui/logcount/queryTag/$lastMonth*/* > /data/push_tag/tmp.simpleMatch.txt
hdfs dfs -text /user/zhangzhonghui/logcount/queryTag/$month*/* >> /data/push_tag/tmp.simpleMatch.txt

sort /data/push_tag/tmp.simpleMatch.txt | python simpleMatch.py w2cs.txt > /data/push_tag/$today/users.tags.tmp

sort /data/push_tag/$today/users.tags.tmp | uniq > /data/push_tag/$today/users.tags

rm /data/push_tag/$today/users.tags.tmp

#cat urlSend.txt | python package.py testPackWithUrlType > /data/push_tag/$today/message.cmd
#python proUrlSend2.py  #新的生成方式

