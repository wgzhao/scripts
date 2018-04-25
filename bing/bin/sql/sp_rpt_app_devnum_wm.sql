
insert overwrite table bing.rpt_app_devnum_wm partition (statis_week='${statisweek_firstday}')
select 
count(device_id) as devnum,
count(case when app_id='2' then device_id end) as android_devnum,
count(case when app_id='4' then device_id end) as iphone_devnum,
count(case when app_id='6' then device_id end) as ipad_devnum,
count(case when first_day between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59' then device_id end) as addnum,
count(case when app_id='2' and first_day between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59' then device_id end) as android_addnum,
count(case when app_id='4' and first_day between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59' then device_id end) as iphone_addnum,
count(case when app_id='6' and first_day between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59' then device_id end) as ipad_addnum
from bing.dw_app_device_ds
where first_day <= '${statisweek_lastday} 23:59:59'
and app_id in ('2','4','6') and isactive=1
;
