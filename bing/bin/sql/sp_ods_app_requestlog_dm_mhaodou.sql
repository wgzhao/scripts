
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set names='utf8';

add jar hdfs://hdcluster/udf/decodeUtil-1.0.jar;
create temporary function geturljson as 'com.haodou.dc.hive.udf.UrlParamDecode';

--创建应用请求信息临时表
create table if not exists bing.tmp_app_requestlog_dm_mhaodou
(
  request_time timestamp comment '请求发起时间 格式：yyyy-mm-dd hh:mi:ss', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '应用版本',
  userid string comment '用户ID。未登录或未知登录用户为空。', 
  function_id string comment '请求调用的函数', 
  parameter_info string comment 'JSON格式的请求参数。由url参数转换而来',
  dev_uuid string comment '设备UUID。'
) comment '从m.haodou.com抽取的应用请求信息'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--创建有效UUID设备临时表
create table if not exists bing.tmp_app_device_dm_uuid
(
  app_id     string comment '好豆应用ID',
  version_id string comment '版本。', 
  dev_uuid   string comment '设备UUID',
  device_id  string comment '好豆设备ID',
  channel_id string comment '渠道ID。格式：渠道编码+版本。',
  userid     string comment '登录用户ID'
) comment '有效UUID设备临时表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--从m.haodou.com抽取小组帖子内容页应用请求信息
insert overwrite table bing.tmp_app_requestlog_dm_mhaodou partition (statis_date='${statis_date}')
select /*+ mapjoin(tt)*/
ll.request_time,
ll.userip,
ll.app_id,
vv.vn as version_id,
ll.userid,
case when `path` like '/topic-%.html?id=%' then 'mhaodou.viewtopic' 
when `path` like '/app/recipe/act/novice.php%' then 'mhaodou.novice'
when `path` like '/native/mall/index.php%' then 'mhaodou.mallindex'
when `path` like '/native/mall/goods.php%' then 'mhaodou.mallgoods'
when `path` like '/native/mall/commodity.php%' then 'mhaodou.mallcommodity'
when `path` like '/native/mall/album.php%' then 'mhaodou.mallalbum'
when `path` like '/native/mall/commodityorder.php%' then 'mhaodou.mallcommodityorder'
when `path` like '/native/mall/order.php%' then 'mhaodou.mallorder'
end as function_id,
ll.parameter_info,
ll.dev_uuid
from
(select
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
remote_addr as userip,
'2' as app_id,
case when user_id!='-' then user_id end as userid,
`path` as path,
parse_url(concat('http://',`host`,`path`),'QUERY','id') as topicid,
parse_url(concat('http://',`host`,`path`),'QUERY','do') as act,
geturljson(concat('http://m.haodou.com',`path`)) as parameter_info,
regexp_extract(http_user_agent, 'HAODOU_RECIPE_CLIENT\\s[0-9]{1,}\\s([0-9]{1,})', 1) as vc,
regexp_extract(http_user_agent, 'HAODOU_RECIPE_CLIENT\\s[0-9]{1,}\\s[0-9]{1,}\\s([0-9a-zA-Z]{1,})', 1) as dev_uuid
from logs.m_haodou_com
where logdate='${statis_date}'
and method='GET' and http_user_agent like '%HAODOU_RECIPE_CLIENT%'
and (`path` like '/topic-%.html?id=%'
or `path` like '/app/recipe/act/novice.php%'
or `path` like '/native/mall/index.php%'
or `path` like '/native/mall/goods.php%'
or `path` like '/native/mall/commodity.php%'
or `path` like '/native/mall/album.php%'
or `path` like '/native/mall/commodityorder.php%'
or `path` like '/native/mall/order.php%'
)) ll 
left outer join bing.dw_app_vnvc vv on (ll.app_id=vv.app_id and ll.vc=vv.vc)
;

--抽取有效UUID设备信息
insert overwrite table bing.tmp_app_device_dm_uuid partition (statis_date='${statis_date}')
select app_id, first_version as version_id, dev_uuid,
min(device_id) as unique_device_id, 
min(channel_id) as unique_channel_id, 
min(first_userid) as userid
from bing.dw_app_device_dm
where statis_date='${statis_date}'
group by app_id, first_version, dev_uuid
having count(device_id)=1
;

--写入应用请求日志
insert overwrite table bing.ods_app_requestlog_dm partition (statis_date='${statis_date}', source_type='m.haodou.com')
select
mm.request_time,
dd.device_id,
dd.channel_id,
mm.userip,
mm.app_id,
mm.version_id,
coalesce(mm.userid,dd.userid),
mm.function_id,
mm.parameter_info,
'' as parameter_desc,
mm.dev_uuid
from
(select *
from bing.tmp_app_requestlog_dm_mhaodou
where statis_date='${statis_date}'
and nvl(dev_uuid,'')!=''
) mm inner join
(select *
from bing.tmp_app_device_dm_uuid
where statis_date='${statis_date}'
) dd on (mm.app_id=dd.app_id and mm.version_id=dd.version_id and mm.dev_uuid=dd.dev_uuid)
;
