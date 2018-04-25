#为营养健康推送选择最新用户
#保证conf.py选择itemBank.tmp.txt

title=妈妈们看过来
type=[食疗]
type1=[人群]
day=20150521
day1=2015-05-21
if [ -z $day1 ]; then
	echo "参数不够!"
	exit 1
fi

url="http://group.haodou.com/topic-329950.html"

echo $title"	"$url"	_special_"$day"	"$(python readTag.py $type $type1)  > itemBank.tmp.txt

#sh match.sh tmp
hdfs dfs -text /user/zhangzhonghui/logcount/push/userTags/tmp/p* | grep topic-329950 | grep -v default  > /data/push_tag/randomUser/nutrition"$day".txt

echo $day1"	http://group.haodou.com/topic-329950.html	"$title"	nutrition"$day >> random.conf

