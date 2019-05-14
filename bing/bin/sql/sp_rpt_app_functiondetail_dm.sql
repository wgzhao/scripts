
insert overwrite table bing.rpt_app_functiondetail_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, function_id,
count(1) as call_cnt,
count(distinct device_id) as dev_num
from bing.ods_app_requestlog_dm
where statis_date='${statis_date}'
group by app_id, function_id
;

insert into table bing.rpt_app_functiondetail_dm partition (statis_date='${statis_date}')
select app_id, version_id, function_id,
count(1) as call_cnt,
count(distinct device_id) as dev_num
from bing.ods_app_requestlog_dm
where statis_date='${statis_date}'
group by app_id, version_id, function_id
;
