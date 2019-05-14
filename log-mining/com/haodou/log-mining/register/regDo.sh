#用户注册后所做的事情

today=`date +%Y%m%d`
for((i=180;i< 270;i+=10))
	do
		day=`date -d -"$i"day +%Y-%m-%d`
hive -e "select userid from hd_haodou_passport_"$today".User where regtime between '"$day" 00:00:00' and '"$day" 23:59:59'" > ~/data/reg/regOkUser.$day
	hdfs dfs -text /user/yarn/logs/source-log.php.CDA39907//$day/* | python regDo.py /home/zhangzhonghui/data/reg/regOkUser.$day > ~/data/reg/regDo.$day

done
