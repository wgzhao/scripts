
insert overwrite table bing.rpt_app_visitdetail_dm partition (statis_date='${statis_date}')
select app_id, '*' as version_id, pagecode,
count(1) as visit_cnt,
count(distinct dev_uuid) as dev_num,
avg(visit_dur) as avg_dur
from (
select get_json_object(json_msg,'$.b.e') as app_id,
get_json_object(json_msg,'$.b.d') as version_id,
get_json_object(json_msg,'$.b.a') as dev_uuid,
get_json_object(json_msg,'$.ext.page') as pagecode,
0 as visit_dur
from bing.ods_app_actionlog_raw_dm
where statis_date='${statis_date}'
and get_json_object(json_msg,'$.ext.action') in ('A1000','A1002')
) t
group by app_id, pagecode
;

insert into table bing.rpt_app_visitdetail_dm partition (statis_date='${statis_date}')
select app_id, version_id, pagecode,
count(1) as visit_cnt,
count(distinct dev_uuid) as dev_num,
avg(visit_dur) as avg_dur
from (
select get_json_object(json_msg,'$.b.e') as app_id,
get_json_object(json_msg,'$.b.d') as version_id,
get_json_object(json_msg,'$.b.a') as dev_uuid,
get_json_object(json_msg,'$.ext.page') as pagecode,
0 as visit_dur
from bing.ods_app_actionlog_raw_dm
where statis_date='${statis_date}'
and get_json_object(json_msg,'$.ext.action') in ('A1000','A1002')
) t
group by app_id, version_id, pagecode
;
