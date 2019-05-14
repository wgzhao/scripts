
--//创建数据库//--
create database bing comment '新BI库' location '/bing/data';


--//系统表//--
create table bing.dual (x string) comment '虚表';
insert into table bing.dual select x from (select count(1) as cnt, 'x' as x from bing.dual) t;


--//管理数据部分（MNG）//--



--//原始数据部分（ODS）//--

--应用请求日志（APPLOG）
drop table bing.ods_app_requestlog_dm;
create table bing.ods_app_requestlog_dm
(
  request_time timestamp comment '请求发起时间', 
  device_id    string comment '设备ID。iOS 设备ID 格式：设备MAC 地址的MD5+IDFA+IDFV。安卓设备ID 格式：haodou+设备IMEI。', 
  channel_id   string comment '渠道ID。格式：渠道编码+版本。', 
  userip string comment '用户访问IP', 
  app_id string comment '应用ID。取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad', 
  version_id string comment '版本。', 
  userid string comment '用户ID。未登录用户为0。', 
  function_id string comment '请求调用的函数', 
  parameter_desc string comment '请求传递的参数', 
  uuid string comment '设备UUID'
) comment '应用请求日志'
  partitioned by (statis_date string)
  clustered by (app_id, device_id)
  sorted by (request_time asc)
  into 16 buckets
  row format delimited fields terminated by '\001' stored as orc
;


--应用行为日志
drop table bing.ods_app_actionlog_raw_dm;
create table bing.ods_app_actionlog_raw_dm
(
  json_msg   string
) comment '应用行为日志'
  partitioned by (statis_date string)
;


--去哪吃城市分站表
--说明：由去哪吃开发提供数据更新
drop table bing.ods_qnc_citysite;
create table bing.ods_qnc_citysite
(
  cityid       int comment '内部城市ID',
  cityname     string comment '城市简称',
  abbrpinyin   string comment '简拼',
  fullname     string comment '城市全称',
  fullpinyin   string comment '全拼',
  domain       string comment '去哪吃分站域名',
  createtime   timestamp comment '数据创建时间'
) comment '去哪吃城市分站表'
  row format delimited fields terminated by '\t' stored as textfile
;

--去哪吃城市分站表数据初始化
--scp /Users/zhong/workspace/githome/bing/install/ods_qnc_citysite.dat zhongpeng@123.150.200.216:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/ods_qnc_citysite.dat' overwrite into table bing.ods_qnc_citysite;
--insert overwrite table bing.ods_qnc_citysite
--select cityid, cityname, abbrpinyin, fullname, fullpinyin, concat(abbrpinyin,'.haodou.com'), from_unixtime(unix_timestamp(),'yyyy-MM-dd HH:mm:ss')
--from bing.ods_qnc_citysite
;


--好豆菜谱达人
--说明：菜谱运营提供数据 2014-09-01改为从haodou_center.VipUser出，过程 sp_ods_rcp_expert
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

--好豆菜谱达人数据初始化
--scp /Users/zhong/workspace/githome/bing/install/ods_rcp_expert.dat zhongpeng@123.150.200.216:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/ods_rcp_expert.dat' overwrite into table bing.ods_rcp_expert;



--应用页面维表
drop table bing.ods_app_page_dim;
create table bing.ods_app_page_dim
(
  app_id       string comment '好豆应用ID 取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad',
  version_id   string comment '版本ID 默认填*表示不分版本',
  pagecode     string comment '应用页面编码',
  pagename     string comment '页面名称'
) comment '应用页面维表'
  row format delimited fields terminated by '\t' stored as textfile
;

--应用页面维表数据初始化
--scp /Users/zhong/workspace/githome/bing/install/ods_app_page_dim.dat zhongpeng@123.150.200.216:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/ods_app_page_dim.dat' overwrite into table bing.ods_app_page_dim;

--应用功能函数维表
drop table bing.ods_app_function_dim;
create table bing.ods_app_function_dim
(
  app_id       string comment '好豆应用ID 取值，1:去哪吃iphone/2:菜谱安卓/3:去哪吃安卓/4:菜谱iphone/5:华为机顶盒/6:菜谱ipad',
  version_id   string comment '版本ID 默认填*表示不分版本',
  function_id  string comment '应用功能函数编码',
  function_name string comment '功能函数名称'
) comment '应用功能函数维表'
  row format delimited fields terminated by '\t' stored as textfile
;

--应用功能函数维表数据初始化
--scp /Users/zhong/workspace/githome/bing/install/ods_app_function_dim.dat zhongpeng@123.150.200.216:/home/dc/bing/install
--load data local inpath '/home/dc/bing/install/ods_app_function_dim.dat' overwrite into table bing.ods_app_function_dim;


--//数据仓库部分（DW）//--

--事件记录
--说明：提供相关产品运营开发的活动信息
drop table bing.ods_qnc_cit;
create table bing.ods_qnc_citysite
(
  cityid       int comment '内部城市ID',
  cityname     string comment '城市简称',
  abbrpinyin   string comment '简拼',
  fullname     string comment '城市全称',
  fullpinyin   string comment '全拼',
  domain       string comment '去哪吃分站域名',
  createtime   timestamp comment '数据创建时间'
) comment '去哪吃城市分站表'
  row format delimited fields terminated by '\t' stored as textfile
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
  dev_uuid     string comment '安卓设备UUID',
  mac_md5      string comment '苹果设备MAC 的MD5 值',
  idfa         string comment '苹果设备IDFA',
  idfv         string comment '苹果设备IDFV',
  req_cnt      int comment '访问请求次数',
  eff_reqcnt   int comment '有效访问次数',
  isvirtual    tinyint comment '虚拟机标识（未出）。取值：1，是，0，否（缺省）',
  isfake       tinyint comment '虚假标识。取值：1，是，0，否（缺省）'
) comment '活跃设备日表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--渠道虚假活跃IP表
drop table bing.dw_app_fake_ip;
create table bing.dw_app_fake_ip
(
  app_id       string comment '好豆应用ID',
  channel_id   string comment '渠道ID。格式：渠道编码+版本。',
  userip       string comment '虚假活跃IP地址（PK）',
  dev_num      int comment '该IP 下的活跃设备数',
  req_cnt      int comment '该IP 下的访问请求次数'
) comment '渠道虚假活跃IP表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--设备总表
drop table bing.dw_app_device_ds;
create table bing.dw_app_device_ds
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  first_day     timestamp comment '首次活跃日期。格式：yyyy-mm-dd hh24:mi:ss。',
  first_channel string comment '首次活跃渠道ID。格式：渠道编码+版本。',
  first_version string comment '首次活跃版本ID',
  first_userip  string comment '首次活跃访问IP',
  first_userid  string comment '首次登录用户ID',
  last_day      timestamp comment '最近活跃日期。格式：yyyy-mm-dd hh24:mi:ss。',
  last_channel  string comment '最近活跃渠道ID。格式：渠道编码+版本。',
  last_version  string comment '最近活跃版本ID',
  last_userip   string comment '最近活跃访问IP',
  last_userid   string comment '最近登录用户ID',
  dev_imei      string comment '安卓设备IMEI',
  dev_uuid      string comment '安卓设备UUID',
  mac_md5       string comment '苹果设备MAC 的MD5 值',
  idfa          string comment '苹果设备IDFA',
  idfv          string comment '苹果设备IDFV',
  virtual       string comment '虚拟机判断信息（未出）',
  issilent      tinyint comment '沉默用户标识（未出）。取值：0，否（缺省）／1，是',
  isvirtual     tinyint comment '虚拟机用户标识（未出）。取值：0，否（缺省）／1，是',
  isfake        tinyint comment '虚假标识。取值：0，否（缺省）／1，是',
  uninst_date   timestamp comment '卸载日期。格式：yyyy-mm-dd。'
) comment '设备总表（外部表）'
  row format delimited fields terminated by '\001' stored as textfile
;

--设备总表（备份表）
drop table bing.dw_app_device_ds_bak;
create table bing.dw_app_device_ds_bak
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  first_day     timestamp comment '首次活跃日期。格式：yyyy-mm-dd hh24:mi:ss。',
  first_channel string comment '首次活跃渠道ID。格式：渠道编码+版本。',
  first_version string comment '首次活跃版本ID',
  first_userip  string comment '首次活跃访问IP',
  first_userid  string comment '首次登录用户ID',
  last_day      timestamp comment '最近活跃日期。格式：yyyy-mm-dd。',
  last_channel  string comment '最近活跃渠道ID。格式：渠道编码+版本。',
  last_version  string comment '最近活跃版本ID',
  last_userip   string comment '最近活跃访问IP',
  last_userid   string comment '最近登录用户ID',
  dev_imei      string comment '安卓设备IMEI',
  dev_uuid      string comment '安卓设备UUID',
  mac_md5       string comment '苹果设备MAC 的MD5 值',
  idfa          string comment '苹果设备IDFA',
  idfv          string comment '苹果设备IDFV',
  virtual       string comment '虚拟机判断信息',
  issilent      tinyint comment '沉默用户标识。取值：0，否（缺省）／1，是',
  isvirtual     tinyint comment '虚拟机用户标识。取值：0，否（缺省）／1，是',
  isfake        tinyint comment '虚假标识。取值：0，否（缺省）／1，是',
  uninst_date   timestamp comment '卸载日期。格式：yyyy-mm-dd。'
) comment '设备总表（备份表）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--设备留存表
drop table bing.dw_app_device_remain_ds;
create table bing.dw_app_device_remain_ds
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  first_day     timestamp comment '首次活跃日期。格式：yyyy-mm-dd hh24:mi:ss。',
  first_channel string comment '首次活跃渠道ID。格式：渠道编码+版本。',
  first_version string comment '首次活跃版本ID',
  remainlog     string comment '留存日志串。缺省：000~000（60个0）。对应天数活跃，相应天位置改为1。'
) comment '设备留存表'
  row format delimited fields terminated by '\001' stored as textfile 
;


--设备留存表（备份表）
drop table bing.dw_app_device_remain_ds_bak;
create table bing.dw_app_device_remain_ds_bak
(
  app_id        string comment '好豆应用ID',
  device_id     string comment '好豆设备ID',
  first_day     timestamp comment '首次活跃日期。格式：yyyy-mm-dd hh24:mi:ss。',
  first_channel string comment '首次活跃渠道ID。格式：渠道编码+版本。',
  first_version string comment '首次活跃版本ID',
  remainlog     string comment '留存日志串。缺省：000~000（60个0）。对应天数活跃，相应天位置改为1。'
) comment '设备留存表（备份表）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile 
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


--手机评论请求日志
drop table bing.dw_app_applog_comment_dm;
create table bing.dw_app_applog_comment_dm
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


--//报表部分（RPT）//--

--应用日常指标
drop table bing.rpt_app_index_dm;
create table bing.rpt_app_index_dm
(
  app_id        string comment '好豆应用ID',
  version_id    string comment '版本ID 默认填*表示不分版本',
  index_name    string comment '指标名称 日留存用户数/日新增用户数/日活跃用户数/日请求用户数/日平均单次使用时长/日启动次数/累计用户/过去7天活跃用户/过去30天活跃用户',
  index_value   double comment '指标值 排除刷量/虚拟机',
  index_value0  double comment '原始指标值'  
) comment '应用日常指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用日常指标（分渠道）
drop table bing.rpt_app_channel_index_dm;
create table bing.rpt_app_channel_index_dm
(
  app_id        string comment '好豆应用ID',
  channel_id    string comment '渠道ID。格式：渠道编码+版本。',
  version_id    string comment '版本ID',
  index_name    string comment '指标名称 日留存用户数/日新增用户数/日活跃用户数/日平均单次使用时长/日启动次数/累计用户/过去7天活跃用户/过去30天活跃用户',
  index_value   double comment '指标值 排除刷量/虚拟机',
  index_value0  double comment '原始指标值'  
) comment '应用日常指标（分渠道）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用日留存分析
drop table bing.rpt_app_remain_dm;
create table bing.rpt_app_remain_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  new_devnum      double comment '新增用户（设备）数',
  remain_devnum1  double comment '1天留存设备数',
  remain_devnum2  double comment '2天留存设备数',
  remain_devnum3  double comment '3天留存设备数',
  remain_devnum4  double comment '4天留存设备数',
  remain_devnum5  double comment '5天留存设备数',
  remain_devnum6  double comment '6天留存设备数',
  remain_devnum7  double comment '7天留存设备数',
  remain_devnum14 double comment '14天留存设备数',
  remain_devnum30 double comment '30天留存设备数'
) comment '应用日留存分析'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用日留存分析（分渠道）
drop table bing.rpt_app_channel_remain_dm;
create table bing.rpt_app_channel_remain_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  new_devnum      double comment '新增用户（设备）数',
  remain_devnum1  double comment '1天留存设备数',
  remain_devnum2  double comment '2天留存设备数',
  remain_devnum3  double comment '3天留存设备数',
  remain_devnum4  double comment '4天留存设备数',
  remain_devnum5  double comment '5天留存设备数',
  remain_devnum6  double comment '6天留存设备数',
  remain_devnum7  double comment '7天留存设备数',
  remain_devnum14 double comment '14天留存设备数',
  remain_devnum30 double comment '30天留存设备数'
) comment '应用日留存分析（分渠道）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用周留存分析
drop table bing.rpt_app_remain_wm;
create table bing.rpt_app_remain_wm
(
  app_id          string comment '好豆应用ID',
  new_devnum      double comment '新增用户（设备）数',
  remain_devnum1  double comment '1周留存设备数',
  remain_devnum2  double comment '2周留存设备数',
  remain_devnum3  double comment '3周留存设备数',
  remain_devnum4  double comment '4周留存设备数',
  remain_devnum5  double comment '5周留存设备数',
  remain_devnum6  double comment '6周留存设备数',
  remain_devnum7  double comment '7周留存设备数'
) comment '应用周留存分析'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\001' stored as textfile
;


--应用功能调用详情
drop table bing.rpt_app_functiondetail_dm;
create table bing.rpt_app_functiondetail_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  function_id     string comment '应用功能函数编码',
  call_cnt        double comment '调用次数',
  dev_num         double comment '调用设备数'
) comment '应用功能调用详情'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用分时段活跃情况
drop table bing.rpt_app_hour_active_dm;
create table bing.rpt_app_hour_active_dm
(
  app_id          string comment '好豆应用ID *表示不分应用',
  act_hour        string comment '时段 取值0~23，*表示不分时段',
  req_cnt         int comment '请求次数',
  req_devnum      int comment '请求设备数',
  eff_req_cnt     int comment '有效请求次数 不含push服务发出的mobiledevice.initandroiddevice请求次数',
  eff_req_devnum  int comment '活跃设备数 不含仅发出push服务的mobiledevice.initandroiddevice请求的设备数'
) comment '应用分时段活跃情况'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--应用页面访问详情
drop table bing.rpt_app_visitdetail_dm;
create table bing.rpt_app_visitdetail_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  pagecode         string comment '应用页面编码',
  visit_cnt       double comment '访问次数 包括：进入页面:A1000/恢复页面:A1002',
  dev_num         double comment '访问设备数',
  avg_dur         double comment '平均单次访问时长 单位：秒'
) comment '应用页面访问详情'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--菜谱广场用户活跃报表
drop table bing.rpt_app_groupact_dm;
create table bing.rpt_app__dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  小组板块
  活动类型
  活动量
  活动人数
  
  pagecode         string comment '应用页面编码',
  visit_cnt       double comment '访问次数 包括：进入页面:A1000/恢复页面:A1002',
  dev_num         double comment '访问设备数',
  avg_dur         double comment '平均单次访问时长 单位：秒'
) comment '应用页面访问详情'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;



--//索引部分（IDX）//--
use bing;

create index idx_ods_qnc_citysite on table ods_qnc_citysite (cityid) 
  as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' with deferred rebuild
  in table idx_ods_qnc_citysite
  location '/bing/idx/idx_ods_qnc_citysite';
alter index idx_ods_qnc_citysite on ods_qnc_citysite rebuild;



