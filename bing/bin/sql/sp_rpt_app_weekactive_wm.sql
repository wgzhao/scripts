
insert overwrite table bing.rpt_app_weekactive_wm partition (statis_week='${statisweek_firstday}')
select 
count(distinct case when eff_reqcnt>0 then device_id end) as actnum,
count(distinct case when app_id='2' and eff_reqcnt>0 then device_id end) as android_actnum,
count(distinct case when app_id='4' and eff_reqcnt>0 then device_id end) as iphone_actnum,
count(distinct case when app_id='6' and eff_reqcnt>0 then device_id end) as ipad_actnum,
count(distinct device_id) as reqnum,
count(distinct case when app_id='2' then device_id end) as android_reqnum,
count(distinct case when app_id='4' then device_id end) as iphone_reqnum,
count(distinct case when app_id='6' then device_id end) as ipad_reqnum
from bing.dw_app_device_dm
where statis_date between '${statisweek_firstday}' and '${statisweek_lastday}'
and app_id in ('2','4','6')
;
