cd /home/zhangzhonghui/new-log-mining/log-mining/com/haodou/log-mining/push/
source ../conf.sh

d2=`date -d -1day +%Y-%m-%d`
sh evaluate.sh $d2
if [ $? -ne 0 ]; then
	echo "评估脚本失败!"
	exit 1
fi

grep random $home_local_dir/data/push/$d2/score.detail.txt > $home_local_dir/data/push/$d2/push.random.hitRate

#更新随机推送下的点击率
cat $home_local_dir/data/push/*/score.detail.txt | grep random | python updateRate.py > itemRate.random.txt 
python ../util/txt2excel.py $home_local_dir/data/push/$d2/score.txt,$home_local_dir/data/push/$d2/push.random.hitRate,itemRate.random.txt 随机推送点击率.$d2.xls 不同策略点击率,$d2随机推送,汇总表

python ../util/MailUtil2.py "zhangzhonghui@haodou.com" 随机推送点击率 请参见附件 随机推送点击率.$d2.xls
#python ../util/MailUtil2.py "jiangjuan@haodou.com" 随机推送点击率 请参见附件 随机推送点击率.$d2.xls
python ../util/MailUtil2.py "xiangjin@haodou.com" 随机推送点击率 请参见附件 随机推送点击率.$d2.xls
#python ../util/MailUtil2.py "chenjingjing@haodou.com" 随机推送点击率 请参见附件 随机推送点击率.$d2.xls
#python ../util/MailUtil2.py "yanxiaoli@haodou.com" 随机推送点击率 请参见附件 随机推送点击率.$d2.xls

mv 随机推送点击率.$d2.xls $home_local_dir/data/push/$d2/


