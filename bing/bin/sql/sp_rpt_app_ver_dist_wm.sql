
set hive.optimize.bucketmapjoin.sortedmerge=false;
set names='utf8';

insert overwrite table bing.rpt_app_ver_dist_wm partition (statis_week='${statisweek_firstday}')
select /*+ mapjoin(t,s)*/
case when t.app_id='2' then '2.菜谱安卓' when t.app_id='4' then '4.菜谱iphone' when t.app_id='6' then '6.菜谱ipad' end as app_name, 
t.version_id, t.devnum, concat(round(100.0*t.devnum/s.act_devnum,2),'%') as vrate
from
(select app_id, version_id, act_devnum as devnum
from bing.rpt_app_dayactive_dm
where statis_date='${statisweek_lastday}' 
and app_id in ('2','4','6') and coalesce(version_id,'NULL')!='*'
) t inner join
(select app_id, act_devnum
from bing.rpt_app_dayactive_dm
where statis_date='${statisweek_lastday}' 
and app_id in ('2','4','6') and coalesce(version_id,'NULL')='*'
) s on (t.app_id=s.app_id)
where (100.0*t.devnum/s.act_devnum)>=1.0
;
