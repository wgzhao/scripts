
set mapreduce.map.memory.mb=8192;
set mapreduce.reduce.memory.mb=8192;
set names='utf8';

create table if not exists bing.tmp_app_channel_index_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  new_devnum      int comment '新增设备激活量',
  base_devnum     int comment '昨日新增设备激活量',
  remain_devnum   int comment '次日留存设备数',
  remain_rate     float comment '次日留存率',
  act_devnum      int comment '活跃设备数',
  avg_dur         int comment '单次使用时长'
) comment '渠道监控指标临时表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

insert overwrite table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select /*+ mapjoin(ip)*/
ds.app_id, ds.channel_id, ds.version_id,
count(distinct ds.device_id) as new_devnum, 
0 as base_devnum,
0 as remain_devnum,
0 as remain_rate,
0 as act_devnum,
0 as avg_dur
from
(select app_id, first_channel as channel_id, first_version as version_id, first_userip as userip, device_id
from bing.dw_app_device_ds
where first_day between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
and isactive=1
) ds left outer join
(select app_id, channel_id, userip
from bing.dw_app_fake_ip
where statis_date='${statis_date}'
and dev_num>=12
) ip on (ds.app_id=ip.app_id and ds.channel_id=ip.channel_id and ds.userip=ip.userip)
where ip.userip is null
group by ds.app_id, ds.channel_id, ds.version_id
;

insert into table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select nd.app_id, nd.channel_id, nd.version_id,
0 as new_devnum,
count(distinct nd.device_id) as base_devnum, 
count(distinct dm.device_id) as remain_devnum,
round(100.0*count(distinct dm.device_id)/count(distinct nd.device_id),2) as remain_rate,
0 as act_devnum,
0 as avg_dur
from 
(select /*+ mapjoin(ip)*/
ds.app_id, ds.channel_id, ds.version_id, ds.device_id
from
(select app_id, first_channel as channel_id, first_version as version_id, first_userip as userip, device_id
from bing.dw_app_device_ds
where first_day between '${preday_date} 00:00:00' and '${preday_date} 23:59:59'
and isactive=1
) ds left outer join
(select app_id, channel_id, userip
from bing.dw_app_fake_ip
where statis_date='${preday_date}'
and dev_num>=12
) ip on (ds.app_id=ip.app_id and ds.channel_id=ip.channel_id and ds.userip=ip.userip)
where ip.userip is null
) nd
left outer join
(select app_id, device_id
from bing.dw_app_device_dm
where statis_date='${statis_date}'
and eff_reqcnt>0
) dm on (nd.app_id=dm.app_id and nd.device_id=dm.device_id)
group by nd.app_id, nd.channel_id, nd.version_id
;

insert into table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select app_id, channel_id, first_version as version_id,
0 as new_devnum,
0 as base_devnum,
0 as remain_devnum,
0 as remain_rate,
count(case when eff_reqcnt>0 then device_id end) as act_devnum,
0 as avg_dur
from bing.dw_app_device_dm
where statis_date='${statis_date}'
group by app_id, channel_id, first_version
;

insert into table bing.tmp_app_channel_index_dm partition (statis_date='${statis_date}')
select app_id, channel_id, version_id,
0 as new_devnum, 
0 as base_devnum,
0 as remain_devnum,
0 as remain_rate,
0 as act_devnum,
avg_dur
from bing.rpt_app_channel_daydur_dm
where statis_date='${statis_date}'
;

insert overwrite table bing.rpt_app_channel_index_dm partition (statis_date='${statis_date}')
select app_id, channel_id, version_id,
sum(new_devnum), 
sum(base_devnum),
sum(remain_devnum),
sum(remain_rate),
sum(act_devnum),
sum(avg_dur)
from bing.tmp_app_channel_index_dm
group by app_id, channel_id, version_id
;
