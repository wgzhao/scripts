
--好豆菜谱入网时长分布报表
insert overwrite table bing.rpt_app_indays_dist_dm partition (statis_date='${statis_date}')
select
dm.app_id, 
ds.indays,
sum(dm.eff_reqcnt) as call_cnt,
count(dm.device_id) as dev_num
from (
select distinct app_id, device_id, eff_reqcnt 
from bing.dw_app_device_dm 
where statis_date='${statis_date}' and eff_reqcnt>0
distribute by app_id, device_id
) dm inner join 
(select app_id, device_id,
datediff('${statis_date}', to_date(first_day)) as indays
from bing.dw_app_device_ds
where first_day<='${statis_date} 23:59:59'
and last_day>='${statis_date} 00:00:00'
distribute by app_id, device_id
) ds on (dm.app_id=ds.app_id and dm.device_id=ds.device_id)
group by dm.app_id, ds.indays
;
