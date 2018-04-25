
set names='utf8';

insert overwrite table bing.rpt_app_avgdev_wm partition (statis_week='${statisweek_firstday}')
select /*+ mapjoin(t)*/ 
avg(android_actnum)+avg(iphone_actnum)+avg(ipad_actnum) as actnum,
avg(android_actnum) as android_actnum,
avg(iphone_actnum) as iphone_actnum,
avg(ipad_actnum) as ipad_actnum,
avg(android_reqnum)+avg(iphone_reqnum)+avg(ipad_reqnum) as reqnum,
avg(android_reqnum) as android_reqnum,
avg(iphone_reqnum) as iphone_reqnum,
avg(ipad_reqnum) as ipad_reqnum
from
(select statis_date,
count(distinct case when app_id='2' and eff_reqcnt>0 then device_id end) as android_actnum,
count(distinct case when app_id='4' and eff_reqcnt>0 then device_id end) as iphone_actnum,
count(distinct case when app_id='6' and eff_reqcnt>0 then device_id end) as ipad_actnum,
count(distinct case when app_id='2' then device_id end) as android_reqnum,
count(distinct case when app_id='4' then device_id end) as iphone_reqnum,
count(distinct case when app_id='6' then device_id end) as ipad_reqnum
from bing.dw_app_device_dm
where statis_date between '${statisweek_firstday}' and '${statisweek_lastday}'
and app_id in ('2','4','6')
group by statis_date
) t
;
