
--应用请求日志
drop table bing.tmp_app_requestlog_dm;
create table bing.tmp_app_requestlog_dm
(
  source_type string comment '数据来源。取值：applog或nginx请求的域名（如api.qunachi.com）。',
  request_time timestamp comment '请求发起时间 格式：yyyy-mm-dd hh:mi:ss', 
  device_id    string comment '设备ID。iOS 设备ID 格式：设备MAC 地址的MD5+IDFA+IDFV。安卓设备ID 格式：haodou+设备IMEI。', 
  channel_id   string comment '渠道ID。格式：渠道编码+版本。', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '版本。', 
  userid string comment '用户ID。未登录或未知登录用户为空。', 
  function_id string comment '请求调用的函数', 
  parameter_desc string comment '请求传递的参数', 
  uuid string comment '设备UUID。'
) comment '应用请求日志'
  partitioned by (statis_date string)
  clustered by (app_id, device_id)
  sorted by (request_time asc)
  into 16 buckets
  row format delimited fields terminated by '\001' stored as orc
;


--活跃设备日表
drop table bing.tmp_app_device_dm;
create table bing.tmp_app_device_dm
(
  app_id       string ,
  device_id    string ,
  channel_id   string ,
  first_time   timestamp ,
  first_version string ,
  first_userip string ,
  first_userid string ,
  dev_imei     string ,
  dev_uuid     string ,
  mac_md5      string ,
  idfa         string ,
  idfv         string ,
  req_cnt      int ,
  eff_reqcnt   int ,
  isvirtual    tinyint ,
  isfake       tinyint 
) 
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--渠道虚假活跃IP表
drop table bing.tmp_app_fake_ip;
create table bing.tmp_app_fake_ip
(
  app_id       string ,
  channel_id   string ,
  userip       string ,
  dev_num      int ,
  req_cnt      int 
) 
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--设备总表
drop table bing.tmp_app_device_ds;
create table bing.tmp_app_device_ds
(
  app_id        string ,
  device_id     string ,
  first_day     timestamp ,
  first_channel string ,
  first_version string ,
  first_userip  string ,
  first_userid  string ,
  last_day      timestamp ,
  last_channel  string ,
  last_version  string ,
  last_userip   string ,
  last_userid   string ,
  dev_imei      string ,
  dev_uuid      string ,
  mac_md5       string ,
  idfa          string ,
  idfv          string ,
  virtual       string ,
  issilent      tinyint ,
  isvirtual     tinyint ,
  isfake        tinyint ,
  uninst_date   timestamp ,
  promo_code    string
) 
  row format delimited fields terminated by '\001' stored as textfile
;

--活跃时长日表
drop table bing.tmp_app_device_duration_dm;
create table bing.tmp_app_device_duration_dm
(
  app_id        string ,
  device_id     string ,
  channel_id    string ,
  version_id    string ,
  session_id    int ,
  request_time  timestamp ,
  request_cnt   int ,
  duration      int 
) 
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--设备留存表
drop table bing.tmp_app_device_remain_ds;
create table bing.tmp_app_device_remain_ds
(
  app_id        string ,
  device_id     string ,
  first_day     timestamp ,
  first_channel string ,
  first_version string ,
  remainlog     string 
) 
  row format delimited fields terminated by '\001' stored as textfile 
;
