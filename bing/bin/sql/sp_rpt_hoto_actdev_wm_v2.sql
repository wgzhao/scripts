
insert overwrite table bing.rpt_hoto_actdev_wm_v2 partition (statis_date='${statisweek_firstday}')
select /*+ mapjoin(t)*/ 
android_num+iphone_num+ipad_num as actnum,
android_num as android_devnum,
iphone_num as iphone_devnum,
ipad_num as ipad_devnum
from
(select
count(distinct case when app_id='2' then device_id end) as android_num,
count(distinct case when app_id='4' then device_id end) as iphone_num,
count(distinct case when app_id='6' then device_id end) as ipad_num
from bing.dw_app_device_dm
where statis_date between '${statisweek_firstday}' and '${statisweek_lastday}'
and app_id in ('2','4','6') and eff_reqcnt>0
) t
;
