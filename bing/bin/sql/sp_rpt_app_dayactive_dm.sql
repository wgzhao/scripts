
set names='utf8';

--不分版本活跃
insert overwrite table bing.rpt_app_dayactive_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id,
count(device_id) as req_devnum,
count(case when eff_reqcnt>0 then device_id end) as act_devnum
from bing.dw_app_device_dm
where statis_date='${statis_date}'
group by app_id
;

--分版本活跃
insert into table bing.rpt_app_dayactive_dm partition (statis_date='${statis_date}')
select app_id, first_version as version_id,
count(device_id) as req_devnum,
count(case when eff_reqcnt>0 then device_id end) as act_devnum
from bing.dw_app_device_dm
where statis_date='${statis_date}'
group by app_id, first_version
;
