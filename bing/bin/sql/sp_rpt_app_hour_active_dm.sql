
insert overwrite table bing.rpt_app_hour_active_dm partition (statis_date='${statis_date}')
select app_id, act_hour, 
count(1) as req_cnt, 
count(distinct device_id) as req_devnum,
count(case when function_id not in ('ad.getad_imocha','common.gettime','mobiledevice.initandroiddevice','mobiledevice.initiphonedevice','notice.getpullnotice','recipe.getfindrecipe','fix.getimagehostdnslist') then 1 end) as eff_req_cnt, 
count(distinct case when function_id not in ('ad.getad_imocha','common.gettime','mobiledevice.initandroiddevice','mobiledevice.initiphonedevice','notice.getpullnotice','recipe.getfindrecipe','fix.getimagehostdnslist') then device_id end) as eff_req_devnum
from (
select hour(request_time) as act_hour, app_id, device_id, function_id
from bing.ods_app_requestlog_dm
where statis_date='${statis_date}'
and app_id in ('1','2','3','4','6')
) t
group by app_id, act_hour
;
