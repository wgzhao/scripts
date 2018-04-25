
s=$1
e=$2
files=""
names=""
for((i=$s;i<=$e;i++))
	do
	
	today=`date -d -"$i"day +%Y-%m-%d`

	hdfs dfs -text /user/zhangzhonghui/logcount/posHit/$today/p* | python readPosHit.py recom-视频菜谱 > /home/zhangzhonghui/data/video/pos.video.$today
	
	files=$files",/home/zhangzhonghui/data/video/pos.video."$today
	names=$names","$today"详表"
	done

python querySeqHit.py favRate  > /home/zhangzhonghui/data/favRate.txt

head -1 /home/zhangzhonghui/data/favRate.txt > /home/zhangzhonghui/data/video/favRate.txt

cat /home/zhangzhonghui/data/favRate.txt | grep 视频菜谱 | grep all_version >> /home/zhangzhonghui/data/video/favRate.txt

files="/home/zhangzhonghui/data/video/favRate.txt"$files
names="视频栏目总体点击收藏情况"$names

start=`date -d -"$e"day +%Y-%m-%d`
end=`date -d -"$s"day +%Y-%m-%d`
python /home/zhangzhonghui/log-mining/com/haodou/log-mining/util/txt2excel.py $files /home/zhangzhonghui/data/video/视频菜谱周报.$start"-"$end.xls $names

function send()
{

address=$1
python /home/zhangzhonghui/log-mining/com/haodou/log-mining/util/MailUtil2.py $address 视频周报	请参见附件	/home/zhangzhonghui/data/video/视频菜谱周报.$start"-"$end.xls


}

send zhangzhonghui@haodou.com
send lijie@haodou.com
send zhongpeng@haodou.com
send zhouxiwei@haodou.com
send liyanlin@haodou.com
send jiangjuan@haodou.com
send huangxinyu@haodou.com


