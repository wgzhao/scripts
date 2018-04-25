
insert overwrite table bing.dw_app_device_reglogin_dm partition (statis_date='${statis_date}')
select app_id, device_id, uuid as dev_uuid, channel_id, version_id, session_id,
max(case when userid!='' then userid end) as userid,
min(request_time) as begin_time,
max(request_time) as end_time,
count(*) as request_cnt,
unix_timestamp(max(request_time))-unix_timestamp(min(request_time)) as request_dur,
concat_ws(',',collect_set(case when function_id in ('common.sendcode','passport.bindconnectstatus',
'passport.connectbindreg','passport.loginbyconnect','passport.reg') then function_id end)) as request_info
from bing.dw_app_requestlog_reg_dm
where statis_date='${statis_date}'
group by app_id, device_id, uuid, channel_id, version_id, session_id
;
