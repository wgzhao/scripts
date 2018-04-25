today=`date -d 1day +%Y-%m-%d`

cd /home/zhangzhonghui/new-log-mining/log-mining/com/haodou/log-mining/push/

sh runNoLog.sh $today > /data/push_tag/log/log.$today 2> /data/push_tag/log/err.$today

