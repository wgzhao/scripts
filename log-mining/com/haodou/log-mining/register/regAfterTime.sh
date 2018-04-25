
#hive -e "select dev_uuid,first_userid,first_day from bing.dw_app_device_ds where dev_uuid != 'NULL' and dev_uuid != ''" > uuid.afterTime
#
#puthon readRegAfterTime.py uuid.afterTime > uid.afterTime
#
#hive -e "select userid,regtime from hd_haodou_passport_20150306.user" > uid.regTime

today=20150311
for((i=30;i< 240;i+=1))
	do
day=`date -d -"$i"day +%Y-%m-%d`

hive -e "select to_date(first_day), count(userid) as user_num, avg(dev_num) as vg_devnum from (select ds.last_userid as userid, min(ds.first_day) as first_day, count(ds.device_id) as  dev_num from bing.dw_app_device_ds ds left semi join (select userid from hd_haodou_passport_"$today".User where regtime between '"$day" 00:00:00' and '"$day" 23:59:59') u on (ds.last_userid=u.userid) group by ds.last_userid ) t group by to_date(first_day);" > ~/data/reg/regAfte.$day
cat ~/data/reg/regAfte.$day | python regAfterTime.py $day >> ~/data/reg/newReg.txt
done

