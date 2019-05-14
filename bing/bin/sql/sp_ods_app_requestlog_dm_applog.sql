
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set names='utf8';

add jar hdfs://hdcluster/udf/bing.jar;
create temporary function getapppara as 'com.haodou.hive.bing.getAppPara';

add jar hdfs://hdcluster/udf/decodeUtil-1.0.jar;
create temporary function getphpjson as 'com.haodou.dc.hive.udf.PhpDecode';
create temporary function geturljson as 'com.haodou.dc.hive.udf.UrlParamDecode';

insert overwrite table bing.ods_app_requestlog_dm partition (statis_date='${statis_date}', source_type='applog')
select from_unixtime(cast(request_time as bigint)) as request_time, 
case when uuid is null or uuid='' then device_id else concat(device_id,'|',uuid) end as device_id, 
channel_id, userip, appid as app_id, version_id, 
case when userid!='0' then userid when getapppara(parameter_desc,'uid') not in ('0','') then getapppara(parameter_desc,'uid') end as userid, 
function_id, getphpjson(parameter_desc) as parameter_info,
parameter_desc,
case when uuid!='' then uuid end as uuid
from logs.log_php_app_log
where logdate='${statis_date}' and cast(request_time as bigint)>0 and device_id is not null 
and channel_id is not null and userip is not null and appid is not null and function_id is not null
;
