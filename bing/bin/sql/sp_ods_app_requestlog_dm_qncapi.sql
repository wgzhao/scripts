
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set names='utf8';

add jar hdfs://hdcluster/udf/bing.jar;
create temporary function getapppara as 'com.haodou.hive.bing.getAppPara';

add jar hdfs://hdcluster/udf/decodeUtil-1.0.jar;
create temporary function getphpjson as 'com.haodou.dc.hive.udf.PhpDecode';
create temporary function geturljson as 'com.haodou.dc.hive.udf.UrlParamDecode';

insert overwrite table bing.ods_app_requestlog_dm partition (statis_date='${statis_date}', source_type='api.qunachi.com')
select
from_unixtime(unix_timestamp(case 
when log_time like '%Jan%' then translate(log_time,'Jan','01')
when log_time like '%Feb%' then translate(log_time,'Feb','02')
when log_time like '%Mar%' then translate(log_time,'Mar','03')
when log_time like '%Apr%' then translate(log_time,'Apr','04')
when log_time like '%May%' then translate(log_time,'May','05')
when log_time like '%Jun%' then translate(log_time,'Jun','06')
when log_time like '%Jul%' then translate(log_time,'Jul','07')
when log_time like '%Aug%' then translate(log_time,'Aug','08')
when log_time like '%Sep%' then translate(log_time,'Sep','09')
when log_time like '%Oct%' then translate(log_time,'Oct','10')
when log_time like '%Nov%' then translate(log_time,'Nov','11')
when log_time like '%Dec%' then translate(log_time,'Dec','12')
end,'dd/MM/yyyy:HH:mm:ss Z')) as request_time, 
translate(parse_url(concat('http://api.qunachi.com',`path`),'QUERY','deviceid'),'%7C','|') as device_id, 
parse_url(concat('http://api.qunachi.com',`path`),'QUERY','channel') as channel_id, 
remote_addr as userip, 
parse_url(concat('http://api.qunachi.com',`path`),'QUERY','appid') as app_id, 
case when parse_url(concat('http://api.qunachi.com',`path`),'QUERY','channel') like '%_v%' then substr(parse_url(concat('http://api.qunachi.com',`path`),'QUERY','channel'), instr(parse_url(concat('http://api.qunachi.com',`path`),'QUERY','channel'),'_v')+1) end as version_id, 
null as userid, 
substr(parse_url(concat('http://api.qunachi.com',`path`),'PATH'),locate('/',parse_url(concat('http://api.qunachi.com',`path`),'PATH'),2)) as function_id, 
geturljson(concat('http://api.qunachi.com',`path`)) as parameter_info,
`path` as parameter_desc,
null as uuid
from logs.api_qunachi_com
where logdate='${statis_date}'
and method='GET' and path rlike '^/v+[0-9]'
;
