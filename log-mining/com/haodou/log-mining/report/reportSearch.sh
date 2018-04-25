
rm ~/data/searchReport.txt

s=2
e=9

cd /home/zhangzhonghui/log-mining/com/haodou/log-mining/log

sh ./testSearchKeyword45.sh $s $e 1

cd /home/zhangzhonghui/log-mining/com/haodou/log-mining/report

for((i=$s;i<$e;i+=1))
do
	today=`date -d -"$i"day +%Y-%m-%d`
	
	hdfs dfs -text /user/zhangzhonghui/logcount/searchKeyword45/$today/p* >> ~/data/searchReport.txt

done

e=`expr $e - 1`
sd=`date -d -"$s"day +%m%d`
ed=`date -d -"$e"day +%m%d`

cat ~/data/searchReport.txt | python reportSearch.py > ~/data/searchReport.$ed-$sd

python ../util/txt2excel.py ~/data/searchReport.$ed-$sd 主动搜索点击周报.$ed-$sd.xls $ed-$sd


python ../util/MailUtil2.py "zhangzhonghui@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
#python ../util/MailUtil2.py "zhaopan@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
#python ../util/MailUtil2.py "jiangjuan@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "yanxiaoli@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
#python ../util/MailUtil2.py "zengrui@haodou.com"  搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "liyanlin@haodou.com"  搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "chenjingjing@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "huangsiqi@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "liujuan@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "hanghong@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls
python ../util/MailUtil2.py "zhongyanhua@haodou.com" 搜索周报 请参见附件 主动搜索点击周报.$ed-$sd.xls


mv 主动搜索点击周报.$ed-$sd.xls ~/data/searchReport


