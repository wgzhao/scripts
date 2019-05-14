
insert overwrite table bing.rpt_hoto_devnum_dm partition (statis_date='${statis_date}')
select 
count(device_id) as devnum,
count(case when app_id='2' then device_id end) as android_devnum,
count(case when app_id='4' then device_id end) as iphone_devnum,
count(case when app_id='6' then device_id end) as ipad_devnum
from bing.dw_app_device_ds
where first_day <= '${statis_date} 23:59:59'
and app_id in ('2','4','6') and isactive=1
;
