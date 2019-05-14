
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;

insert overwrite table bing.dw_applog_recipedownload_dm partition (statis_date='${statis_date}')
select
t.request_time,
t.device_id,
t.channel_id,
t.userip, 
t.app_id, 
t.version_id, 
t.userid, 
t.function_id, 
t.parameter_info,
t.parameter_desc, 
t.uuid,
get_json_object(t.parameter_info,'$.rid') as recipeid,
get_json_object(t.parameter_info,'$.request_id') as request_id,
'{}' as resp_info
from bing.ods_app_requestlog_dm t
where statis_date='${statis_date}' 
and function_id='info.downloadinfo'
;
