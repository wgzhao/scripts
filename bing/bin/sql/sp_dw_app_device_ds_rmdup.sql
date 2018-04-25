
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set hive.groupby.skewindata=true;
set hive.auto.convert.join=false;
set names='utf8';

--清中间表
drop table if exists bing.dw_app_device_ds_dup;
create table if not exists bing.dw_app_device_ds_dup like bing.dw_app_device_ds;

--抽取应用2_安卓菜谱重复设备（在imei后追加uuid）
insert overwrite table bing.dw_app_device_ds_dup
select ds.*
from bing.dw_app_device_ds ds
left semi join
(select dev_imei
from bing.dw_app_device_ds
where app_id='2'
group by dev_imei
having sum(case when dev_uuid='' then 1 else 2 end)=3
) dup on (ds.app_id='2' and ds.dev_imei=dup.dev_imei)
;

--非重复设备写回
insert overwrite table bing.dw_app_device_ds
select /*+ mapjoin(du)*/ ds.* 
from bing.dw_app_device_ds ds
left outer join
(select app_id, device_id from bing.dw_app_device_ds_dup) du on (ds.app_id=du.app_id and ds.device_id=du.device_id)
where du.device_id is null
;

--应用2_安卓菜谱重复设备去重写回
insert into table bing.dw_app_device_ds
select '2' as app_id,
min(case when t.dsn=1 then t.device_id end) as device_id,
min(case when t.sn=1 then t.first_day end) as first_day,
min(case when t.sn=1 then t.first_channel end) as first_channel,
min(case when t.sn=1 then t.first_version end) as first_version,
min(case when t.sn=1 then t.first_userip end) as first_userip,
min(case when t.sn=1 then t.first_userid end) as first_userid,
min(case when t.dsn=1 then t.last_day end) as last_day,
min(case when t.dsn=1 then t.last_channel end) as last_channel,
min(case when t.dsn=1 then t.last_version end) as last_version,
min(case when t.dsn=1 then t.last_userip end) as last_userip,
min(case when t.dsn=1 then t.last_userid end) as last_userid,
t.dev_imei,
min(case when t.dsn=1 then t.dev_uuid end) as dev_uuid,
min(case when t.dsn=1 then t.mac_md5 end) as mac_md5,
min(case when t.dsn=1 then t.mac_idfa end) as mac_idfa,
min(case when t.dsn=1 then t.mac_idfv end) as mac_idfv,
'' as virtual,       
0 as isvirtual,     
null as uninst_date
from
(select du.*, 
row_number() over(partition by du.dev_imei order by du.first_day asc) as sn,
row_number() over(partition by du.dev_imei order by du.last_day desc) as dsn
from bing.dw_app_device_ds_dup du
where du.app_id='2'
) t
group by t.dev_imei
;
