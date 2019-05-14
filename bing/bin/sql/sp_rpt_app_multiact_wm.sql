
--应用周多次活跃用户数
--周多次活跃用户指当周活跃天数在2天及以上的用户
insert overwrite table bing.rpt_app_multiact_wm partition (statis_week='${statisweek_firstday}')
select
sum(case when app_id=1 then dev_num end) as app1_devnum,
sum(case when app_id=2 then dev_num end) as app2_devnum,
sum(case when app_id=3 then dev_num end) as app3_devnum,
sum(case when app_id=4 then dev_num end) as app4_devnum,
coalesce(sum(case when app_id=5 then dev_num end),0) as app5_devnum,
coalesce(sum(case when app_id=6 then dev_num end),0) as app6_devnum
from (
select app_id, count(device_id) as dev_num
from (
select app_id, device_id, count(distinct statis_date) as days
from bing.dw_app_device_dm
where statis_date between '${statisweek_firstday}' and '${statisweek_lastday}'
and eff_reqcnt>0
group by app_id, device_id
) t
where days>1
group by app_id
) tt
;
