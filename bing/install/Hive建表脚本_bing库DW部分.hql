

--//数据仓库部分（DW）//--


--应用版本对应关系表
--从api的nignx日志中抽取
drop table bing.dw_app_vnvc;
create table bing.dw_app_vnvc
(
  app_id  string comment '好豆应用ID',
  vc      string comment '版本vc',
  vn      string comment '版本vn',
  update_date string comment '维护更新日期',
  update_cnt  int comment '维护更新日的请求发生次数'
) comment '应用版本对应关系表'
  row format delimited fields terminated by '\t' stored as textfile
;

--应用功能列表
drop table bing.dw_app_function;
create table bing.dw_app_function
(
  app_id          string comment '好豆应用ID',
  function_id     string comment '函数id',
  function_name   string comment '函数名',
  function_desc   string comment '函数说明',
  first_version   string comment '首次出现版本ID',
  first_date      string comment '首次出现日期。格式yyyy-mm-dd',
  function_mode   string comment '函数行为模式。取值:b:background/a:action/p:page'
  value_factor    int comment '价值系数。默认-1未设定,0无价值,1-5价值从低到高'
) comment '应用功能列表'
  row format delimited fields terminated by '\t' stored as textfile
;

--已知应用功能列表
drop table bing.dw_app_known_function;
create table bing.dw_app_known_function
(
  app_id          string comment '好豆应用ID',
  function_id     string comment '函数id',
  function_name   string comment '函数名',
  value_factor    int comment '价值系数。默认-1未设定,0无价值,1-5价值从低到高'
) comment '已知应用功能列表'
  row format delimited fields terminated by '\t' stored as textfile
;
--已知应用功能列表数据初始化
--scp /Users/zhong/workspace/githome/bing/install/dw_app_known_function.dat zhongpeng@123.150.200.216:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/dw_app_known_function.dat' overwrite into table bing.dw_app_known_function;


--应用页面列表
drop table bing.dw_app_page;
create table bing.dw_app_page
(
  app_id          string comment '好豆应用ID',
  page_code       string comment '页面编码',
  page_full       string comment '页面编码全名',
  page_name       string comment '页面名',
  page_desc       string comment '页面说明'
) comment '应用功能列表'
  row format delimited fields terminated by '\t' stored as textfile
;
create table bing.tmp_app_page
(
  app_id          string comment '好豆应用ID',
  page_code       string comment '页面编码',
  page_full       string comment '页面编码全名',
  page_name       string comment '页面名',
  page_desc       string comment '页面说明'
) comment '应用功能列表'
  row format delimited fields terminated by '\t' stored as textfile
;
--加载
--scp ~/workspace/githome/bing/install/dw_app_page1013.dat tj16:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/dw_app_page1013.dat' overwrite into table bing.tmp_app_page;
--检查
select app_id, page_code
from bing.tmp_app_page
group by app_id, page_code
having count(1)>1
;
--同步
insert overwrite table bing.dw_app_page
select 
coalesce(pt.app_id,p.app_id), 
coalesce(pt.page_code,p.page_code), 
coalesce(pt.page_full,p.page_full),
coalesce(pt.page_name,p.page_name), 
coalesce(pt.page_desc,p.page_desc)
from bing.dw_app_page p
full join bing.tmp_app_page pt on (p.app_id=pt.app_id and p.page_code=pt.page_code)
;

--设备信息日表
drop table bing.dw_app_devinfo_dm;
create table bing.dw_app_devinfo_dm
(
  app_id       string comment '好豆应用ID',
  device_id    string comment '好豆设备ID',
  dev_uuid     string comment '设备UUID',
  dev_imei     string comment '设备IMEI',
  dev_mac      string comment '设备MAC地址',
  dev_idfa     string comment '苹果设备IDFA',
  dev_idfv     string comment '苹果设备IDFV',
  dev_brand    string comment '设备品牌',
  dev_model    string comment '设备型号',
  dev_os       string comment '设备操作系统',
  dev_osver    string comment '设备操作系统版本',
  scr_width    string comment '设备屏幕宽。安卓设备单位为像素pixels，苹果设备单位为pts。',
  scr_height   string comment '设备屏幕高。安卓设备单位为像素pixels，苹果设备单位为pts。',
  operator_name string comment '运营商名称',
  operator_code string comment '运营商代码',
  dev_rooted   string comment '设备ROOTED。0:false，1:true'
) comment '设备信息日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--设备信息总表
drop table bing.dw_app_devinfo_ds;
create table bing.dw_app_devinfo_ds
(
  app_id       string comment '好豆应用ID',
  device_id    string comment '好豆设备ID',
  dev_uuid     string comment '设备UUID',
  dev_imei     string comment '设备IMEI',
  dev_mac      string comment '设备MAC地址',
  dev_idfa     string comment '苹果设备IDFA',
  dev_idfv     string comment '苹果设备IDFV',
  dev_brand    string comment '设备品牌',
  dev_model    string comment '设备型号',
  dev_os       string comment '设备操作系统',
  dev_osver    string comment '设备操作系统版本',
  scr_width    string comment '设备屏幕宽。安卓设备单位为像素pixels，苹果设备单位为pts。',
  scr_height   string comment '设备屏幕高。安卓设备单位为像素pixels，苹果设备单位为pts。',
  operator_name string comment '运营商名称',
  operator_code string comment '运营商代码',
  dev_rooted   string comment '设备ROOTED。0:false，1:true',
  first_date   string comment '首次活跃日期。',
  last_date    string comment '最近活跃日期。'
) comment '设备信息日表'
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--活跃设备日表
drop table bing.dw_app_device_dm;
create table bing.dw_app_device_dm
(
  app_id       string comment '好豆应用ID',
  device_id    string comment '好豆设备ID',
  channel_id   string comment '渠道ID。格式：渠道编码+版本。',
  first_time   timestamp comment '首次活跃时间。格式：yyyy-mm-dd hh24:mi:ss。',
  first_version string comment '当天首次版本ID',
  first_userip string comment '当天首次访问IP',
  first_userid string comment '当天首次登录用户ID',
  dev_imei     string comment '安卓设备IMEI',
  dev_uuid     string comment '设备UUID',
  mac_md5      string comment '苹果设备网卡地址的MD5 值',
  mac_idfa     string comment '苹果设备IDFA',
  mac_idfv     string comment '苹果设备IDFV',
  req_cnt      int comment '访问请求次数',
  eff_reqcnt   int comment '有效访问次数',
  isvirtual    tinyint comment '虚拟机标识（未出）。取值：0 否（缺省）/1 是',
  isfake       tinyint comment '虚假标识（未出）。取值：0 否（缺省）/1 单IP设备过多/2 特定IP（福建莆田IP）'
) comment '活跃设备日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--渠道虚假活跃IP表
drop table bing.dw_app_fake_ip;
create table bing.dw_app_fake_ip
(
  app_id       string comment '好豆应用ID',
  channel_id   string comment '渠道ID。格式：渠道编码+版本。',
  userip       string comment '虚假活跃IP地址',
  dev_num      int comment '该IP 下的活跃设备数',
  req_cnt      int comment '该IP 下的访问请求次数'
) comment '渠道虚假活跃IP表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--渠道虚假活跃UUID表
drop table bing.dw_app_fake_uuid;
create table bing.dw_app_fake_uuid
(
  app_id       string comment '好豆应用ID',
  channel_id   string comment '渠道ID。格式：渠道编码+版本。',
  dev_uuid     string comment '虚假活跃UUID',
  dev_num      int comment '该UUID下的活跃设备数',
  req_cnt      int comment '该UUID下的访问请求次数'
) comment '渠道虚假活跃UUID表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--设备总表
drop table bing.dw_app_device_ds;
create table bing.dw_app_device_ds
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  first_day     timestamp comment '首次活跃时间。格式：yyyy-mm-dd hh24:mi:ss。',
  first_channel string comment '首次活跃渠道ID。格式：渠道编码+版本。',
  first_version string comment '首次活跃版本ID',
  first_userip  string comment '首次活跃访问IP',
  first_userid  string comment '首次登录用户ID',
  last_day      timestamp comment '最近活跃时间。格式：yyyy-mm-dd hh24:mi:ss。',
  last_channel  string comment '最近活跃渠道ID。格式：渠道编码+版本。',
  last_version  string comment '最近活跃版本ID',
  last_userip   string comment '最近活跃访问IP',
  last_userid   string comment '最近登录用户ID',
  dev_imei      string comment '安卓设备IMEI',
  dev_uuid      string comment '设备UUID',
  mac_md5       string comment '苹果设备网卡地址的MD5 值',
  mac_idfa      string comment '苹果设备IDFA',
  mac_idfv      string comment '苹果设备IDFV',
  virtual       string comment '虚拟机判断信息（未出）',
  isvirtual     tinyint comment '虚拟机标识（未出）。取值：0 否（缺省）/1 是',
  uninst_date   timestamp comment '卸载日期（未出）。格式：yyyy-mm-dd。',
  isactive      tinyint comment '是否活跃过'
) comment '设备总表'
  row format delimited fields terminated by '\001'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--设备总表（不含uuid）
drop table bing.dw_app_device_nouuid_ds;
create table bing.dw_app_device_nouuid_ds
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  first_day     timestamp comment '首次活跃时间。格式：yyyy-mm-dd hh24:mi:ss。',
  first_channel string comment '首次活跃渠道ID。格式：渠道编码+版本。',
  first_version string comment '首次活跃版本ID',
  first_userip  string comment '首次活跃访问IP',
  first_userid  string comment '首次登录用户ID',
  last_day      timestamp comment '最近活跃时间。格式：yyyy-mm-dd hh24:mi:ss。',
  last_channel  string comment '最近活跃渠道ID。格式：渠道编码+版本。',
  last_version  string comment '最近活跃版本ID',
  last_userip   string comment '最近活跃访问IP',
  last_userid   string comment '最近登录用户ID',
  isactive      tinyint comment '是否活跃过'
) comment '设备总表（不含uuid）'
  row format delimited fields terminated by '\001'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--应用请求会话信息
drop table bing.dw_app_requestlog_session_dm;
create table bing.dw_app_requestlog_session_dm
(
  request_time timestamp comment '请求发起时间 格式：yyyy-mm-dd hh:mi:ss', 
  device_id    string comment '设备ID。iOS 设备ID 格式：设备MAC 地址的MD5+IDFA+IDFV+UUID。安卓设备ID 格式：haodou+设备IMEI+UUID。', 
  channel_id   string comment '渠道ID。格式：渠道编码+版本。', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '版本', 
  userid string comment '用户ID。未登录或未知登录用户为空。', 
  function_id string comment '请求调用的函数', 
  parameter_info string comment 'JSON格式的请求参数。空',
  parameter_desc string comment '原始请求传递的参数。applog中为php序列化数据，nginx中为url参数。空', 
  uuid string comment '设备UUID',
  session_id int comment '会话id',
  session_seq int comment '会话顺序'
) comment '应用请求日志'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--应用注册请求会话信息
drop table bing.dw_app_requestlog_reg_dm;
create table bing.dw_app_requestlog_reg_dm
(
  request_time timestamp comment '请求发起时间 格式：yyyy-mm-dd hh:mi:ss', 
  device_id    string comment '设备ID。iOS 设备ID 格式：设备MAC 地址的MD5+IDFA+IDFV+UUID。安卓设备ID 格式：haodou+设备IMEI+UUID。', 
  channel_id   string comment '渠道ID。格式：渠道编码+版本。', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '版本', 
  userid string comment '用户ID。未登录或未知登录用户为空。', 
  function_id string comment '请求调用的函数', 
  parameter_info string comment 'JSON格式的请求参数。空',
  parameter_desc string comment '原始请求传递的参数。applog中为php序列化数据，nginx中为url参数。空', 
  uuid string comment '设备UUID',
  session_id int comment '会话id',
  session_seq int comment '会话顺序'
) comment '应用注册请求会话信息'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;


--应用访问会话日志
drop table bing.dw_app_accesslog_session_dm;
create table bing.dw_app_accesslog_session_dm
(
  access_time  timestamp comment '设备记录时间。格式：yyyy-mm-dd hh:mi:ss。取$ext分项的time。',
  dev_uuid     string comment '设备uuid值。取$b.a',
  app_id       string comment '应用ID。取$b.d',
  channel_id   string comment '渠道号。取$b.channel',
  version_id   string comment '应用版本。取$b.e',
  userip       string comment '用户访问IP。取$user_ip',
  userid       string comment '用户ID。暂时没有',
  page         string comment '页面编码。取$ext分项的page',
  session_id int comment '会话id',
  session_seq int comment '会话顺序'
) comment '应用访问会话日志'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;



--用户交互会话表（应用请求会话信息的子表，仅包含需要分析的活动）
drop table bing.dw_app_interaction_session_dm;
create table bing.dw_app_interaction_session_dm
(
  request_time timestamp comment '请求发起时间 格式：yyyy-mm-dd hh:mi:ss', 
  device_id    string comment '设备ID。iOS 设备ID 格式：设备MAC 地址的MD5+IDFA+IDFV+UUID。安卓设备ID 格式：haodou+设备IMEI+UUID。', 
  channel_id   string comment '渠道ID。格式：渠道编码+版本。', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '版本', 
  userid string comment '用户ID。未登录或未知登录用户为空。', 
  function_id string comment '请求调用的函数', 
  parameter_info string comment 'JSON格式的请求参数。空',
  parameter_desc string comment '原始请求传递的参数。applog中为php序列化数据，nginx中为url参数。空', 
  uuid string comment '设备UUID',
  session_id int comment '会话id',
  session_seq int comment '会话顺序'
) comment '应用请求日志'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;


--活跃时长日表
drop table bing.dw_app_device_duration_dm;
create table bing.dw_app_device_duration_dm
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  channel_id    string comment '活跃渠道ID。格式：渠道编码+版本。',
  version_id    string comment '活跃版本ID',
  session_id    int comment '当次设备会话ID',
  request_time  timestamp comment '当次会话开始时间。格式：yyyy-mm-dd hh24:mi:ss。',
  request_cnt   int comment '当次会话请求次数',
  duration      int comment '当次会话时长。单位：秒'
) comment '活跃时长日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--注册/登录活动日表
--本表数据来源applog,附加注册用户信息
drop table bing.dw_app_device_reglogin_dm;
create table bing.dw_app_device_reglogin_dm
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  dev_uuid      string comment '好豆设备UUID',  
  channel_id    string comment '活跃渠道ID。格式：渠道编码+版本。',
  version_id    string comment '活跃版本ID',
  session_id    int comment '当次设备会话ID',
  userid        string comment '当次注册/登录用户ID',
  begin_time    timestamp comment '当次活动开始时间。格式：yyyy-mm-dd hh24:mi:ss。',
  end_time      timestamp comment '当次活动结束时间。格式：yyyy-mm-dd hh24:mi:ss。',
  request_cnt   int comment '当次活动请求次数',
  request_dur   int comment '当次活动时长。单位：秒',
  request_info  string comment '当次活动请求信息'
) comment '注册/登录活动日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--手机页面访问行为日表
drop table bing.dw_app_actionlog_visit_dm;
create table bing.dw_app_actionlog_visit_dm
(
  app_id        string comment '好豆应用ID',
  dev_uuid      string comment '设备UUID',
  version_id    string comment '版本ID',
  session_id    int comment '当次设备会话ID',
  seq           int comment '当次会话的页面访问序号',
  pagecode      string comment '当次会话访问的应用页面编码',
  refcode       string comment '上一个页面编码',
  visit_time    timestamp comment '当次会话的页面访问开始时间 格式：yyyy-mm-dd hh24:mi:ss',
  visit_dur     int comment '当前页面停留时长 单位：秒'
) comment '手机页面访问行为日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--ip地址库
drop table bing.dw_iplib;
create table bing.dw_iplib
(
  ip       string comment 'IP地址。格式：aaa.bbb.ccc.ddd',
  city     string comment '城市',
  province string comment '省份',
  country  string comment '国家'
) comment 'ip地址库精简版'
row format delimited fields terminated by '\001' stored as orc
;

-- ip地址库精简版
drop table bing.dw_iplib_lite;
create table bing.dw_iplib_lite
(
  ipseg    string comment 'IP地址前3段。格式：aaa.bbb.ccc.',
  ipsegnum int comment 'IP段值。计算公式：aaa<<24+bbb<<16+ccc<<8',
  city     string comment '城市'
) comment 'ip地址库精简版'
row format delimited fields terminated by '\001' stored as orc
;


-- ip地址库精简版操作记录
drop table bing.dw_iplib_lite_log;
create table bing.dw_iplib_lite_log
(
  logtime  timestamp comment '操作时间',
  rownum   int comment '源表记录数。源表为：logs.ip_address_warehouse'
) comment 'ip地址库精简版操作记录'
row format delimited fields terminated by '\001' stored as textfile
;


--活跃vistor日表
drop table bing.dw_web_vistor_dm;
create table bing.dw_web_vistor_dm
(
  req_time     timestamp comment '请求时间。格式：yyyy-mm-dd hh24:mi:ss。',
  userip       string comment '访问 IP',
  user_agent   string comment '访问 UA',
  domain       string comment '访问域名',
  url          string comment '访问 URL',
  req_cnt      int comment '访问请求次数'
) comment '活跃设备日表'
  partitioned by (statis_date string, site string)
  row format delimited fields terminated by '\t' stored as textfile
;

--菜谱新手课堂菜谱
drop table bing.dw_rcp_freshclass;
create table bing.dw_rcp_freshclass
(
  classid    int comment 'id',
  recipeid   int comment '菜谱id',
  begindate  string comment '开始日期。格式：yyyy-mm-dd',
  enddate    string comment '结束日期。格式：yyyy-mm-dd'
) comment '菜谱新手课堂菜谱'
row format delimited fields terminated by '\t' stored as textfile
;

--手机评论请求日志（从应用请求日志抽取）
drop table bing.dw_applog_comment_dm;
create table bing.dw_applog_comment_dm
(
  request_time  timestamp comment '请求时间',
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  channel_id    string comment '渠道ID。格式：渠道编码+版本。',
  version_id    string comment '版本ID',
  userip        string comment '请求IP',
  userid        string comment '好豆用户ID',
  function_id   string comment '评论请求方法',
  p_itemid      string comment '评论对象ID 0菜谱评论为菜谱id/1菜谱专辑评论为菜谱专辑id/6广场话题为话题id',
  p_replyid     string comment '回复评论ID',
  p_type        string comment '评论类型 0菜谱/1菜谱专辑/5豆记/6广场话题/7菜谱专题/10礼品商城/12成果照/13FEED',
  p_atuserid    string comment '回复评论用户ID',
  p_content     string comment '评论内容'
) comment '手机评论请求日志'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--菜谱下载日志（从应用请求日志抽取）
drop table bing.dw_applog_recipedownload_dm;
create table bing.dw_applog_recipedownload_dm
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
  uuid string comment '设备UUID。',
  recipeid string comment '菜谱id',
  request_id string comment '请求id',
  resp_info  string comment '响应信息'
) comment '菜谱下载日志（从应用请求日志抽取）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;


--城市用户表（外部表）
--在该城市ip活跃过(applog)的用户都算在该城市下。本表中一个用户可能归属多个城市。
drop table bing.dw_haodou_cityuser;
create external table bing.dw_haodou_cityuser
(
  city         string comment '城市',
  province     string comment '省份',
  country      string comment '国家',
  userid       string comment '用户id',
  lastip       string comment '用户在该城市的最后活跃ip',
  lasttime     timestamp comment '用户在该城市的最后活跃时间。格式:yyyy-mm-dd hh:mi:ss',
  source       string comment '数据来源。applog/weblog'
  ) comment '城市用户表（外部表）'
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  location '/bing/ext/dw_haodou_cityuser'
;

--用户城市表（外部表）
--以用户最后登录ip划归用户归属城市。本表中一个用户只归属一个城市。
drop table bing.dw_haodou_usercity;
create external table bing.dw_haodou_usercity
(
  userid       string comment '用户id',
  username     string comment '用户名',
  city         string comment '城市',
  province     string comment '省份',
  country      string comment '国家',
  loginip      string comment '用户最后登录ip',
  logintime    timestamp comment '用户最后登录时间。格式:yyyy-mm-dd hh:mi:ss'
  ) comment '用户城市表（外部表）'
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat'
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
  location '/bing/ext/dw_haodou_usercity'
;

--城市表(geoip)
--使用geoip库生成
drop table bing.dw_haodou_city_geoip;
create external table bing.dw_haodou_city_geoip
(
  city         string comment '城市英文名',
  province     string comment '省份英文名',
  country      string comment '国家英文名',
  city_cn      string comment '城市中文名',
  province_cn  string comment '省份中文名',
  country_cn   string comment '国家中文名'
  ) comment '城市表(geoip)'
  row format delimited fields terminated by '\t' stored as textfile
  location '/bing/ext/dw_haodou_city_geoip'
;


--管理员豆币发放明细记录
drop table bing.dw_haodou_adminlog_doubi;
create table bing.dw_haodou_adminlog_doubi
(
  operid       string comment '管理员id',
  opername     string comment '管理员姓名',
  userlist     string comment '发放对象',
  operinfo     string comment '发放说明',
  opertime     timestamp comment '发放时间',
  usernum      int comment '发放人数',
  perdoubi     int comment '单人豆币数',
  doubi        int comment '当次豆币数'
) comment '管理员豆币发放记录'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;




