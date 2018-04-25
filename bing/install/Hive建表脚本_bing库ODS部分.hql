

--//原始数据部分（ODS）//--

--应用请求日志
--来源：logs.log_php_app_log 以及 logs.api_qunachi_com 的 '/v%'
--因为去哪吃4.0版本开始，没走logs.log_php_app_log
drop table bing.ods_app_requestlog_dm;
create table bing.ods_app_requestlog_dm
(
  request_time timestamp comment '请求发起时间 格式：yyyy-mm-dd hh:mi:ss', 
  device_id    string comment '设备ID。iOS 设备ID 格式：设备MAC 地址的MD5+IDFA+IDFV+UUID。安卓设备ID 格式：haodou+设备IMEI+UUID。', 
  channel_id   string comment '渠道ID。格式：渠道编码+版本。', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '版本。', 
  userid string comment '用户ID。未登录或未知登录用户为空。', 
  function_id string comment '请求调用的函数', 
  parameter_info string comment 'JSON格式的请求参数。',
  parameter_desc string comment '原始请求传递的参数。applog中为php序列化数据，nginx中为url参数', 
  uuid string comment '设备UUID。'
) comment '应用请求日志'
  partitioned by (statis_date string, source_type string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;


--应用行为日志（原生）
drop table bing.ods_app_actionlog_raw_dm;
create table bing.ods_app_actionlog_raw_dm
(
  json_msg   string
) comment '应用行为日志（原生）'
  partitioned by (statis_date string)
  row format serde 'org.apache.hadoop.hive.serde2.lazy.LazySimpleSerDe' 
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--应用行为日志
--参考：http://wiki.haodou.cn/index.php?title=手机APP数据统计技术文档
drop table bing.ods_app_actionlog_dm;
create external table bing.ods_app_actionlog_dm
(
  request_time timestamp comment '设备记录时间。格式：yyyy-mm-dd hh:mi:ss。取$ext分项的time。',
  dev_uuid     string comment '设备uuid值。取$b.a',
  channel_id   string comment '渠道号。取$b.channel',
  userip       string comment '用户访问IP。取$user_ip',
  app_id       string comment '应用ID。取$b.d',
  version_id   string comment '应用版本。取$b.e',
  userid       string comment '用户ID。暂时没有',
  action_code  string comment '动作ID。取$ext分项的action',
  page_code    string comment '页面编码。取$ext分项的page',
  action_info  string comment '动作信息。取$ext分项'      
) comment '应用行为日志'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  location '/bing/ext/ods_app_actionlog_dm'
;

--应用访问日志
drop table bing.ods_app_accesslog_dm;
create external table bing.ods_app_accesslog_dm
(
  access_time  timestamp comment '设备记录时间。格式：yyyy-mm-dd hh:mi:ss。取$ext分项的time。',
  dev_uuid     string comment '设备uuid值。取$b.a',
  app_id       string comment '应用ID。取$b.d',
  channel_id   string comment '渠道号。取$b.channel',
  version_id   string comment '应用版本。取$b.e',
  userip       string comment '用户访问IP。取$user_ip',
  userid       string comment '用户ID。暂时没有',
  page         string comment '页面编码。取$ext分项的page',
  access_info  string comment '动作信息。取$ext分项'      
) comment '应用访问日志'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  location '/bing/ext/ods_app_accesslog_dm'
;


--应用崩溃日志
--来源：应用行为日志 A3001
drop table bing.ods_app_crashlog_raw_dm;
create table bing.ods_app_crashlog_raw_dm
(
  channel_id string comment '渠道号。$b.channel',
  uuid_md5   string comment 'uuid的md5值。$b.a',
  os_model   string comment '设备型号。$b.b',
  os_version string comment '设备版本。$b.c',
  app_id     string comment 'APP。$b.d',
  version_id string comment '设备版本。$b.e',
  dev_mac    string comment '设备MAC。$b.f',
  geo_long   string comment '经度。$b.g',
  geo_lati   string comment '纬度。$b.h',
  net_type   string comment '网络接入类型。$b.i',
  scr_width  string comment '屏幕分辨率-宽。$b.j',
  scr_high   string comment '屏幕分辨率-高。$b.k',
  dev_sn     string comment '设备序列号。$b.l',
  dev_board  string comment '设备BOARD。$b.m',
  dev_loader string comment '设备BOOTLOADER。$b.n',
  dev_brand  string comment '设备BRAND。$b.o',
  dev_device string comment '设备DEVICE。$b.p',
  
) comment '应用崩溃日志'
  partitioned by (statis_date string)
;


--好豆菜谱达人
drop table bing.ods_rcp_expert;
create table bing.ods_rcp_expert
(
  userid       string comment '好豆用户ID',
  username     string comment '用户名',
  expert_type  string comment '达人类型 个人/机构',
  expert_level string comment '达人级别 菜谱达人',
  expert_style string comment '达人菜谱风格',
  eff_date     string comment '生效日期 格式：yyyy-mm-dd',
  exp_date     string comment '失效日期 格式：yyyy-mm-dd',
  status       int comment '状态 1有效/0无效'
) comment '好豆菜谱达人'
  row format delimited fields terminated by '\t' stored as textfile
;


--好豆小组考核用户
--说明：小组运营提供数据 #2015-11-04 取消手动初始化，改为sp_ods_grp_admuser生成（抽取小组管理员/生活盟主）
drop table bing.ods_grp_admuser;
create table bing.ods_grp_admuser
(
  userid       string comment '好豆用户ID',
  username     string comment '用户名',
  group_name   string comment '所属小组名',
  group_title  string comment '小组头衔',
  eff_date     string comment '生效日期 格式：yyyy-mm-dd',
  exp_date     string comment '失效日期 格式：yyyy-mm-dd',
  status       int comment '状态 1有效/0无效'
) comment '好豆菜谱达人'
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆小组考核用户数据初始化
--scp ~/workspace/githome/bing/install/ods_grp_admuser.dat tj16:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/ods_grp_admuser.dat' overwrite into table bing.ods_grp_admuser;



-- ip地址库
drop table bing.ods_iplib;
create table bing.ods_iplib
(
  ip       string comment 'IP地址。格式：aaa.bbb.ccc.ddd',
  ipnum    int comment 'IP值。计算公式：aaa<<24+bbb<<16+ccc<<8+ddd',
  country  string comment '国家',
  area     string comment '省份',
  region   string comment '地市',
  city     string comment '城市',
  county   string comment '区域',
  isp      string comment '电信运营商',
  source   string comment '数据来源。如ip.taobao.com',
  creratetime timestamp comment ''
) comment 'ip地址库'
row format delimited fields terminated by '\t' stored as textfile ;

