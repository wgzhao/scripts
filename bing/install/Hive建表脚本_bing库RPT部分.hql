

--//报表部分（RPT）//--

--##移动端类
--应用日活跃分析
drop table bing.rpt_app_dayactive_dm;
create table bing.rpt_app_dayactive_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  req_devnum      double comment '请求设备数',
  act_devnum      double comment '活跃设备数',
) comment '应用日活跃分析'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用日活跃分析（分渠道）
drop table bing.rpt_app_channel_dayactive_dm;
create table bing.rpt_app_channel_dayactive_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  req_devnum      double comment '请求设备数',
  act_devnum      double comment '活跃设备数'
) comment '应用日活跃分析（分渠道）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用日活跃分析（分）
drop table bing.rpt_app_channel_dayactive_dm;
create table bing.rpt_app_channel_dayactive_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  req_devnum      double comment '请求设备数',
  act_devnum      double comment '活跃设备数'
) comment '应用日活跃分析（分渠道）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用日时长指标
drop table bing.rpt_app_daydur_dm;
create table bing.rpt_app_daydur_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 填*表示不分版本',
  total_dur       int comment '总使用时长',
  total_cnt       int comment '总使用次数 本处所有的活动都不包含后台请求类活动',
  total_devnum    int comment '总使用设备数',
  avg_dur         int comment '单次使用平均时长',
  eff_totaldur    int comment '有效使用总时长 取活动时长在4秒到10000秒之间活动',
  eff_totalcnt    int comment '有效使用总次数 取活动时长在4秒到10000秒之间的活动次数',
  eff_devnum      int comment '有效使用设备数',
  eff_avgdur      int comment '单次有效使用平均时长'
) comment '应用日时长指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--应用日时长指标（分渠道） 
drop table bing.rpt_app_channel_daydur_dm;
create table bing.rpt_app_channel_daydur_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  total_dur       int comment '总使用时长',
  total_cnt       int comment '总使用次数 本处所有的活动都不包含后台请求类活动',
  total_devnum    int comment '总使用设备数',
  avg_dur         int comment '单次使用平均时长',
  eff_totaldur    int comment '有效使用总时长 取活动时长在4秒到10000秒之间活动',
  eff_totalcnt    int comment '有效使用总次数 取活动时长在4秒到10000秒之间的活动次数',
  eff_devnum      int comment '有效使用设备数',
  eff_avgdur      int comment '单次有效使用平均时长'
) comment '应用日时长指标（分渠道）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--应用日单次时长分布分析
drop table bing.rpt_app_daydur_dist_dm;
create table bing.rpt_app_daydur_dist_dm
(
  app_id          string comment '好豆应用ID 分析维度',
  version_id      string comment '版本ID 默认填*表示不分版本。分析维度',
  duration_id     int comment '单次会话时长 分析维度',
  total_dur       int comment '总使用时长',
  total_cnt       int comment '总使用次数 本处所有的活动都不包含后台请求类活动',
  total_devnum    int comment '总使用设备数'
) comment '应用日单次时长分布分析'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
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
  row format delimited fields terminated by '\t' stored as textfile
;

--应用日留存分析中间表
drop table bing.rpt_app_remain_dm_np;
create table bing.rpt_app_remain_dm_np
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
  remain_devnum30 double comment '30天留存设备数',
  statis_date     string
) comment '应用日留存分析中间表'
  partitioned by (exec_date string)
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

--应用日留存分析（分渠道）中间表
drop table bing.rpt_app_channel_remain_dm_np;
create table bing.rpt_app_channel_remain_dm_np
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
  remain_devnum30 double comment '30天留存设备数',
  statis_date     string
) comment '应用日留存分析（分渠道）中间表'
  partitioned by (exec_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--应用周留存分析
drop table bing.rpt_app_remain_wm;
create table bing.rpt_app_remain_wm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  new_devnum      int comment '新增用户（设备）数',
  remain_devnum1  int comment '1周留存设备数',
  remain_devnum2  int comment '2周留存设备数',
  remain_devnum3  int comment '3周留存设备数',
  remain_devnum4  int comment '4周留存设备数',
  remain_devnum5  int comment '5周留存设备数',
  remain_devnum6  int comment '6周留存设备数',
  remain_devnum7  int comment '7周留存设备数'
) comment '应用周留存分析'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--应用周留存分析中间表
drop table bing.rpt_app_remain_wm_np;
create table bing.rpt_app_remain_wm_np
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 默认填*表示不分版本',
  new_devnum      int comment '新增用户（设备）数',
  remain_devnum1  int comment '1周留存设备数',
  remain_devnum2  int comment '2周留存设备数',
  remain_devnum3  int comment '3周留存设备数',
  remain_devnum4  int comment '4周留存设备数',
  remain_devnum5  int comment '5周留存设备数',
  remain_devnum6  int comment '6周留存设备数',
  remain_devnum7  int comment '7周留存设备数',
  statis_week     string
) comment '应用周留存分析中间表'
  partitioned by (exec_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--应用注册时长分析
drop table bing.rpt_app_regdur_dm;
create table bing.rpt_app_regdur_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID 填*表示不分版本',
  total_cnt       int comment '总注册发起次数',
  success_cnt     int comment '成功注册次数',
  avg_dur         int comment '成功注册平均时长',
  email_cnt       int comment '邮箱成功注册次数',
  email_dur       int comment '邮箱成功注册平均时长',
  phone_cnt       int comment '手机成功注册次数',
  phone_dur       int comment '手机成功注册平均时长',
  3p_cnt          int comment '第三方成功注册次数',
  3p_dur          int comment '第三方成功注册平均时长'
) comment '应用注册时长分析'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
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

--应用用户访问路径
drop table bing.rpt_app_accesspath_dm;
create table bing.rpt_app_accesspath_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID',
  node_key        string comment '访问路径节点编码。path的md5',
  node_path       string comment '访问路径。格式：aa:bb:cc...',
  node_page       string comment '访问页面',
  node_level      int comment '访问路径节点层级',
  subnode_num     int comment '子节点数',
  access_cnt      int comment '访问次数',
  bounce_cnt      int comment '跳出次数',
  prior_nodekey   string comment '访问路径父节点' 
) comment '应用用户访问路径'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t'
  stored as inputformat 'com.hadoop.mapred.DeprecatedLzoTextInputFormat' 
  outputformat 'org.apache.hadoop.hive.ql.io.HiveIgnoreKeyTextOutputFormat'
;

--应用分时段活跃情况
drop table bing.rpt_app_hour_active_dm;
create table bing.rpt_app_hour_active_dm
(
  app_id          string comment '好豆应用ID *表示不分应用',
  act_hour        string comment '时段 取值0~23，*表示不分时段',
  req_cnt         int comment '请求次数',
  req_devnum      int comment '请求设备数',
  eff_req_cnt     int comment '有效请求次数',
  eff_req_devnum  int comment '活跃设备数'
) comment '应用分时段活跃情况'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--应用手机操作系统版本分析（分渠道） 
drop table bing.rpt_app_channel_osver_dm;
create table bing.rpt_app_channel_osver_dm
(
  app_id      string comment '好豆应用ID',
  os_version  string comment '操作系统版本ID',
  channel_id  string comment '渠道ID。格式：渠道编码+版本。',
  devnum      int comment '使用设备数'
) comment '应用手机操作系统版本分析（分渠道） '
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
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


--应用图片访问错误情况
drop table bing.rpt_app_imgerr_dm;
create table bing.rpt_app_imgerr_dm
(
  app_id string comment '应用ID',
  host string comment '访问主机',
  errormsg string comment '错误信息。默认*',
  errorcnt int comment '报错次数',
  errordev int comment '报错设备数'
) comment '应用图片访问错误情况'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;


--应用广场用户活跃报表
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
) comment '应用广场用户活跃报表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;


--##好豆BI周报
--好豆BI周报结果表1-应用周多次活跃用户数
--*周多次活跃用户指当周活跃天数在2天及以上的用户
drop table bing.rpt_app_multiact_wm;
create table bing.rpt_app_multiact_wm
(
  app1_devnum     int comment '去哪吃iPhone版',
  app2_devnum     int comment '好豆菜谱Android版',
  app3_devnum     int comment '去哪吃Android版',
  app4_devnum     int comment '好豆菜谱iPhone版',
  app5_devnum     int comment '华为机顶盒',
  app6_devnum     int comment '好豆菜谱iPad版'
) comment '好豆BI周报结果表1-应用周多次活跃用户数'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表2-菜谱成果照发布
--*本处指标按日累计
drop table bing.rpt_rcp_photopublish_wm;
create table bing.rpt_rcp_photopublish_wm
(
  photo_num        int comment '发布成果照总数',
  usr_num          int comment '发布成果照总人数',
  web_photonum     int comment '网站发布成果照数',
  android_photonum int comment '安卓端发布成果照数',
  iphone_photonum  int comment 'iPhone端发布成果照数',   
  web_usrnum       int comment '网站发布成果照人数',
  android_usrnum   int comment '安卓端发布成果照人数',
  iphone_usrnum    int comment 'iPhone端发布成果照人数'     
) comment '好豆BI周报结果表2-菜谱成果照发布'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;


--好豆BI周报结果表3-菜谱发布
--*本处指标按日累计
drop table bing.rpt_rcp_recipepublish_wm;
create table bing.rpt_rcp_recipepublish_wm
(
  recipe_num        int comment '发布菜谱总数',
  usr_num           int comment '发布菜谱总人数',
  web_recipenum     int comment '网站发布菜谱数',
  android_recipenum int comment '安卓端发布菜谱数',
  iphone_recpienum  int comment 'iPhone端发布菜谱数',   
  web_usrnum        int comment '网站发布菜谱人数',
  android_usrnum    int comment '安卓端发布成果照人数',
  iphone_usrnum     int comment 'iPhone端发布成果照人数'  
) comment '好豆BI周报结果表3-菜谱发布'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表4-菜谱应用装机量
drop table bing.rpt_app_devnum_wm;
create table bing.rpt_app_devnum_wm
(
  devnum            int comment '菜谱应用装机累计',
  android_devnum    int comment '安卓菜谱装机累计',
  iphone_devnum     int comment 'iPhone菜谱装机累计',
  ipad_devnum       int comment 'iPad菜谱装机累计',
  addnum            int comment '当周菜谱应用装机量',
  android_addnum    int comment '当周安卓菜谱装机量',
  iphone_addnum     int comment '当周iPhone菜谱装机量',
  ipad_addnum       int comment '当周iPad菜谱装机量'
) comment '好豆BI周报结果表4-菜谱应用装机量'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表5-菜谱应用日均活跃量
drop table bing.rpt_app_avgdev_wm;
create table bing.rpt_app_avgdev_wm
(
  actnum         int comment '日均活跃设备数',
  android_actnum int comment '安卓菜谱活跃设备数',
  iphone_actnum  int comment 'iPhone菜谱活跃设备数',
  ipad_actnum    int comment 'iPad菜谱活跃设备数',
  reqnum         int comment '日均请求设备数',
  android_reqnum int comment '安卓菜谱请求设备数',
  iphone_reqnum  int comment 'iPhone菜谱请求设备数',
  ipad_reqnum    int comment 'iPad菜谱请求设备数'
) comment '好豆BI周报结果表5-菜谱应用日均活跃量'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表6-菜谱应用周活跃量
drop table bing.rpt_app_weekactive_wm;
create table bing.rpt_app_weekactive_wm
(
  actnum         int comment '周跃设备数',
  android_actnum int comment '安卓菜谱周活跃设备数',
  iphone_actnum  int comment 'iPhone菜谱周活跃设备数',
  ipad_actnum    int comment 'iPad菜谱周活跃设备数',
  reqnum         int comment '周请求设备数',
  android_reqnum int comment '安卓菜谱周请求设备数',
  iphone_reqnum  int comment 'iPhone菜谱周请求设备数',
  ipad_reqnum    int comment 'iPad菜谱周请求设备数'
) comment '好豆BI周报结果表6-菜谱应用周活跃量'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表8-菜谱应用日活版本分布
drop table bing.rpt_app_ver_dist_wm;
create table bing.rpt_app_ver_dist_wm
(
  app_name       string comment '应用',
  version_id     string comment '版本',
  devnum         int comment '活跃用户',
  vrate          string comment '版本占比'
) comment '好豆BI周报结果表8-菜谱应用日活版本分布'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;


--好豆BI周报结果表5-广场发帖
drop table bing.rpt_grp_topicpublish_wm;
create table bing.rpt_grp_topicpublish_wm
(
  group_id          string comment '小组ID，不分小组为*',
  topic_num         int comment '发帖数',
  user_num          int comment '发帖人数'
) comment '好豆BI周报结果表5-广场发帖'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表6-广场回复
drop table bing.rpt_grp_comment_wm;
create table bing.rpt_grp_comment_wm
(
  group_id          string comment '小组ID，不分小组为*',
  comment_num       int comment '回复数',
  user_num          int comment '回复人数'
) comment '好豆BI周报结果表6-广场回复'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆BI周报结果表7-小组当周日平均发帖回复
drop table bing.rpt_grp_avgtopicomment_wm;
create table bing.rpt_grp_avgtopicomment_wm
(
  group_id          string comment '小组ID，不分小组为*',
  topic_num         int comment '发帖数',
  topic_user        int comment '发帖人数',
  comment_num       int comment '回复数',
  comment_user      int comment '回复人数'
) comment '好豆BI周报结果表7-小组当周日平均发帖回复'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;



--广场内容浏览情况（分小组）
drop table bing.rpt_grp_topicview_dm;
create table bing.rpt_grp_topicview_dm
(
  source_type   string comment '来源。取值：web/app/wap',
  group_id      string comment '小组ID。不分小组为*',
  visit_cnt     int comment '访问次数',
  ip_num        int comment '独立IP数'
) comment '广场内容浏览情况（分小组）'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--好豆BI周报结果表7-去哪吃探店贴
drop table bing.rpt_qnc_shoptopic_wm;
create table bing.rpt_qnc_shoptopic_wm
(
  city_id           string comment '城市ID。不分城市为*，未指定城市为0',
  topic_num         int comment '探店贴发布数',
  user_num          int comment '探店贴发布人数',
  shop_type_num     int comment '走街寻店贴数',
  snack_type_num    int comment '特色小吃贴数',
  std_mode_num      int comment '标准模式贴数',
  free_mode_num     int comment '自由模式贴数',
  star_num          int comment '2星级及以上贴数'
) comment '好豆BI周报结果表7-去哪吃探店贴'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;


--好豆BI周报结果表8-去哪吃美食发布
drop table bing.rpt_qnc_paishare_wm;
create table bing.rpt_qnc_paishare_wm
(
  city_id           string comment '城市ID。不分城市为*，未指定城市为0',
  share_num         int comment '美食发布数',
  user_num          int comment '美食发布人数',
  web_share_num     int comment '网站发布美食数',
  android_share_num int comment '安卓端发布美食数',
  iphone_share_num  int comment 'iPhone端发布美食数',
  web_user_num      int comment '网站发布美食人数',
  android_user_num  int comment '安卓端发布美食人数',
  iphone_user_num   int comment 'iPhone端发布美食人数'
) comment '好豆BI周报结果表8-去哪吃美食发布'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\001' stored as textfile
;


--##菜谱类

--菜谱发布累计指标
--本处发布数据为通过审核的数据
drop table bing.rpt_rcp_recipepublish_sum;
create table bing.rpt_rcp_recipepublish_sum
(
  recipe_num        int comment '菜谱发布数',
  user_num          int comment '菜谱发布人数',
  web_recipenum     int comment '网站发布菜谱数',
  android_recipenum int comment '安卓端发布菜谱数',
  iphone_recipenum  int comment 'iPhone端发布菜谱数',
  web_usernum       int comment '网站发布菜谱人数',
  android_usernum   int comment '安卓端发布菜谱人数',
  iphone_usernum    int comment 'iPhone端发布菜谱人数',
  star1_num         int comment '1星菜谱数',
  star2_num         int comment '2星菜谱数',
  star3_num         int comment '3星菜谱数',
  star4_num         int comment '4星菜谱数',
  star5_num         int comment '5星菜谱数',
  index_num         int comment '收录菜谱数'
) comment '菜谱发布累计指标'
  row format delimited fields terminated by '\t' stored as textfile
;

--菜谱发布日指标
drop table bing.rpt_rcp_recipepublish_dm;
create table bing.rpt_rcp_recipepublish_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  recipe_num        int comment '菜谱发布数',
  user_num          int comment '菜谱发布人数',
  web_recipenum     int comment '网站发布菜谱数',
  android_recipenum int comment '安卓端发布菜谱数',
  iphone_recipenum  int comment 'iPhone端发布菜谱数',
  web_usernum       int comment '网站发布菜谱人数',
  android_usernum   int comment '安卓端发布菜谱人数',
  iphone_usernum    int comment 'iPhone端发布菜谱人数',
  recipe_passnum    int comment '菜谱发布通过数',
  user_passnum      int comment '菜谱发布通过人数',
  web_passnum       int comment '网站发布菜谱通过数',
  android_passnum   int comment '安卓端发布菜谱通过数',
  iphone_passnum    int comment 'iPhone端发布菜谱通过数',
  web_passusr       int comment '网站发布菜谱通过人数',
  android_passusr   int comment '安卓端发布菜谱通过人数',
  iphone_passusr    int comment 'iPhone端发布菜谱通过人数',
  star_num          int comment '3星及以上菜谱数',
  index_num         int comment '收录菜谱数'
) comment '菜谱发布日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--菜谱评论日指标
drop table bing.rpt_rcp_recipecomment_dm;
create table bing.rpt_rcp_recipecomment_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  comment_num       int comment '菜谱评论数',
  user_num          int comment '菜谱评论人数',
  recipe_num        int comment '被评论菜谱数',
  web_commentnum     int comment '网站发布评论数',
  android_commentnum int comment '安卓端发布评论数',
  iphone_commentnum  int comment 'iPhone端发布评论数',
  web_usernum       int comment '网站发布评论人数',
  android_usernum   int comment '安卓端发布评论人数',
  iphone_usernum    int comment 'iPhone端发布评论人数',
  reply_num         int comment '菜谱评论回复数',
  reply_usernum     int comment '菜谱评论回复人数'
) comment '菜谱评论日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--菜谱喜欢日指标
drop table bing.rpt_rcp_recipelike_dm;
create table bing.rpt_rcp_recipelike_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  like_num          int comment '菜谱喜欢数',
  user_num          int comment '菜谱喜欢人数',
  recipe_num        int comment '被喜欢菜谱数'
) comment '菜谱喜欢日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--菜谱收藏到专辑日指标
drop table bing.rpt_rcp_recipefavorite_dm;
create table bing.rpt_rcp_recipefavorite_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  favorite_num      int comment '收藏次数',
  recipe_num        int comment '被收藏菜谱数',
  user_num          int comment '收藏人数',
  album_num         int comment '收藏活动专辑数'
) comment '菜谱收藏到专辑日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--成果照发布日指标
drop table bing.rpt_rcp_photopublish_dm;
create table bing.rpt_rcp_photopublish_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  photo_num         int comment '发布成果照总数',
  usr_num           int comment '发布成果照总人数',
  web_photonum      int comment '网站发布成果照数',
  android_photonum  int comment '安卓端发布成果照数',
  iphone_photonum   int comment 'iPhone端发布成果照数',   
  web_usrnum        int comment '网站发布成果照人数',
  android_usrnum    int comment '安卓端发布成果照人数',
  iphone_usrnum     int comment 'iPhone端发布成果照人数'     
) comment '成果照发布日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--成果照发布来源
drop table bing.rpt_rcp_photosource_dm;
create table bing.rpt_rcp_photosource_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  source_type       string comment '来源分类',
  source_name       string comment '来源',
  photo_num         int comment '成果照数',
  usr_num           int comment '发布成果照人数',
  loc_photo_num     int comment '其中：带地址成果照数'
) comment '成果照发布日指标'
  row format delimited fields terminated by '\t' stored as textfile
;

--成果照喜欢日指标
drop table bing.rpt_rcp_photolike_dm;
create table bing.rpt_rcp_photolike_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  like_num          int comment '成果照喜欢数',
  usr_num           int comment '喜欢成果照人数',
  photo_num         int comment '被喜欢成果照数'
) comment '成果照喜欢日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--成果照评论日指标
drop table bing.rpt_rcp_photocomment_dm;
create table bing.rpt_rcp_photocomment_dm
(
  statis_date        string comment '统计日期。格式：yyyy-mm-dd',
  comment_num        int comment '成果照评论数',
  usr_num            int comment '发布成果照评论人数',
  web_commentnum     int comment '网站发布评论数',
  android_commentnum int comment '安卓端发布评论数',
  iphone_commentnum  int comment 'iPhone端发布评论数',   
  web_usrnum         int comment '网站发布评论人数',
  android_usrnum     int comment '安卓端发布评论人数',
  iphone_usrnum      int comment 'iPhone端发布评论人数'     
) comment '成果照评论日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--菜谱专辑关注日指标
drop table bing.rpt_rcp_albumfollow_dm;
create table bing.rpt_rcp_albumfollow_dm
(
  statis_date       string comment '统计日期。格式：yyyy-mm-dd',
  follow_num        int comment '菜谱专辑关注数',
  user_num          int comment '菜谱专辑关注人数',
  album_num         int comment '被关注菜谱专辑数'
) comment '菜谱喜欢日指标'
  row format delimited fields terminated by '\t' stored as textfile
;


--##北京bi.hoto.cn使用报表（菜谱）
drop table bing.rpt_hoto_devnum_dm;
create table bing.rpt_hoto_devnum_dm
(
  devnum         int comment '总激活量',
  android_devnum int comment '安卓菜谱总激活量',
  iphone_devnum  int comment 'iPhone菜谱总激活量',
  ipad_devnum    int comment 'iPad菜谱总激活量'
) comment '激活量'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_reguser_dm;
create table bing.rpt_hoto_reguser_dm
(
  usernum  int comment '总注册用户数',
  newuser  int comment '当天注册用户数'
) comment '注册用户数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_reguser_dm_np;
create table bing.rpt_hoto_reguser_dm_np
(
  usernum  int comment '总注册用户数',
  newuser  int comment '当天注册用户数',
  statis_date string
) comment '注册用户数（非分区中间表）'
  partitioned by (exec_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--从mysql表showdb.rpt_user_index同步  yes
drop table bing.rpt_hoto_reguser_dm_v2;
create table bing.rpt_hoto_reguser_dm_v2
(
  statis_date string comment '统计日期',
  usernum  int comment '总注册用户数'
) comment '注册用户数'
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_avgdev_mm;
create table bing.rpt_hoto_avgdev_mm
(
  avgnum         int comment '月日均活跃设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数',
  ipad_devnum    int comment 'iPad菜谱设备数'
) comment '月日均活跃设备数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_avgdev_mm_v2;
create table bing.rpt_hoto_avgdev_mm_v2
(
  avgnum         int comment '月日均请求设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数',
  ipad_devnum    int comment 'iPad菜谱设备数'
) comment '月日均请求设备数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--从mysql表showdb.rpt_dayact_index同步   yes
drop table bing.rpt_hoto_avgdev_mm_v3;   
create table bing.rpt_hoto_avgdev_mm_v3
(
  statis_date string comment '统计日期',
  avgnum         int comment '月日均活跃设备数'
) comment '月日均活跃设备数'
  row format delimited fields terminated by '\t' stored as textfile
;

--从mysql表showdb.rpt_dayact_index同步   yes
drop table bing.rpt_hoto_actdev_dm_v3;
create table bing.rpt_hoto_actdev_dm_v3
(
  statis_date string comment '统计日期',
  app_user    int comment '日活跃设备数'
) comment '日活跃设备数'
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_actdev_mm;
create table bing.rpt_hoto_actdev_mm
(
  actnum         int comment '月活跃设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数',
  ipad_devnum    int comment 'iPad菜谱设备数'
) comment '月活跃设备数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_actdev_mm_v2;
create table bing.rpt_hoto_actdev_mm_v2
(
  actnum         int comment '月请求设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数',
  ipad_devnum    int comment 'iPad菜谱设备数'
) comment '月请求设备数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--从mysql表showdb.rpt_monthact_index同步   yes
drop table bing.rpt_hoto_actdev_mm_v3;
create table bing.rpt_hoto_actdev_mm_v3
(
  statis_date string comment '统计日期',
  actnum         int comment '月请求设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数'
) comment '月请求设备数'
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_actdev_wm;
create table bing.rpt_hoto_actdev_wm
(
  actnum         int comment '周活跃设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数',
  ipad_devnum    int comment 'iPad菜谱设备数'
) comment '周活跃设备数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_actdev_wm_v2;
create table bing.rpt_hoto_actdev_wm_v2
(
  actnum         int comment '周请求设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数',
  ipad_devnum    int comment 'iPad菜谱设备数'
) comment '周请求设备数'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--从mysql表showdb.rpt_monthact_index同步    yes
drop table bing.rpt_hoto_actdev_wm_v3;
create table bing.rpt_hoto_actdev_wm_v3
(
  statis_date string comment '统计日期',
  actnum         int comment '周请求设备数',
  android_devnum int comment '安卓菜谱设备数',
  iphone_devnum  int comment 'iPhone菜谱设备数'
) comment '周活跃设备数'
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_remain_dm;
create table bing.rpt_hoto_remain_dm
(
  newnum         int comment '日新增设备数',
  remain_num     int comment '日留存设备数',
  remain_rate    double comment '日留存率',
  android_newnum int comment '安卓菜谱新增设备数',
  android_remain int comment '安卓菜谱留存设备数',
  android_rate   double comment '安卓菜谱留存率',
  iphone_newnum  int comment 'iPhone菜谱新增设备数',
  iphone_remain  int comment 'iPhone菜谱留存设备数',
  iphone_rate    double comment 'iPhone菜谱留存率',
  ipad_newnum    int comment 'iPad菜谱新增设备数',
  ipad_remain    int comment 'iPad菜谱留存设备数',
  ipad_rate      double comment 'iPad菜谱留存率'
) comment '日留存率'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_hoto_remain_mm;
create table bing.rpt_hoto_remain_mm
(
  newnum         int comment '月新增设备数',
  remain_num     int comment '月留存设备数',
  remain_rate    double comment '月留存率',
  android_newnum int comment '安卓菜谱新增设备数',
  android_remain int comment '安卓菜谱留存设备数',
  android_rate   double comment '安卓菜谱留存率',
  iphone_newnum  int comment 'iPhone菜谱新增设备数',
  iphone_remain  int comment 'iPhone菜谱留存设备数',
  iphone_rate    double comment 'iPhone菜谱留存率',
  ipad_newnum    int comment 'iPad菜谱新增设备数',
  ipad_remain    int comment 'iPad菜谱留存设备数',
  ipad_rate      double comment 'iPad菜谱留存率'
) comment '月留存率'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--从mysql表showdb.rpt_appdur_index同步   yes
drop table bing.rpt_hoto_avgdur_dm;
create table bing.rpt_hoto_avgdur_dm
(
  statis_date string comment '统计日期',
  devnum         int comment '活跃设备数',
  avgdur         int comment '平均使用时长'
) comment '平均使用时长'
  row format delimited fields terminated by '\t' stored as textfile
;


--##其他类

--用户注册日指标
drop table bing.rpt_haodou_reguser_dm;
create table bing.rpt_haodou_reguser_dm
(
  statis_date       string comment '注册日期。格式：yyyy-mm-dd',
  usernum           int comment '注册用户数',
  email_usernum     int comment '邮箱注册用户数',
  phone_usernum     int comment '手机注册用户数',
  3p_usernum        int comment '第3方注册用户数',
  163_usernum       int comment '网易注册用户数',
  tqq_usernum       int comment '腾讯微博注册用户数',
  weibo_usernum     int comment '新浪微博注册用户数',
  douban_usernum    int comment '豆瓣注册用户数',
  sohu_usernum      int comment '搜狐注册用户数',
  qzone_usernum     int comment 'QQ空间注册用户数',
  taobao_usernum    int comment '淘宝注册用户数',
  weixin_usernum    int comment '微信注册用户数',
  3p_nobind         int comment '第3方注册无绑定用户数'
) comment '用户注册日指标'
  row format delimited fields terminated by '\t' stored as textfile
;

/*新增字段数据迁移脚本
create table bing......_new
;

insert overwrite table bing.rpt_haodou_reguser_dm_new
select
statis_date   ,
usernum       ,
email_usernum ,
phone_usernum ,
3p_usernum    ,
163_usernum   ,
tqq_usernum   ,
weibo_usernum ,
douban_usernum,
sohu_usernum  ,
qzone_usernum ,
taobao_usernum,
0 as weixin_usernum,
3p_nobind     
from bing.rpt_haodou_reguser_dm
;

drop table bing.rpt_haodou_reguser_dm;
use bing;
alter table rpt_haodou_reguser_dm_new rename to rpt_haodou_reguser_dm;
*/

--注册用户构成分析（已停）
drop table bing.rpt_haodou_reguser_dm;
create table bing.rpt_haodou_reguser_dm
(
  statis_date       string comment '注册日期。格式：yyyy-mm-dd',
  product_type      string comment '产品来源。0通行证/1菜谱/2去哪吃/3小组/4商城',
  way_type          string comment '注册来路。0邮箱(默认)/1手机/20新浪微博/21腾讯微博/22腾讯QQ/23豆瓣/24网易/25淘宝',
  platform_type     string comment '注册平台。0桌面浏览器/1安卓手机/2苹果手机/3手机浏览器/4微软手机/5安卓平板/6苹果平板',
  email_feature     string comment '邮箱特征串。邮箱域名',
  mobile_feature    string comment '手机特征串。手机号码前3位',
  usernum           int comment '正常注册用户数',
  invalid_usernum   int comment '无效注册用户数'
) comment '注册用户构成分析'
  row format delimited fields terminated by '\t' stored as textfile
;

--短信下发统计日报
drop table bing.rpt_haodou_sendsms_dm;
create table bing.rpt_haodou_sendsms_dm
(
  source_type       string comment '数据来源。haodou(好豆网菜谱)/qunachi(去哪吃)',
  sms_type          string comment '短信类型ID',
  sms_typename      string comment '短信类型名称',
  send_cnt          int comment '发送次数',
  mobile_num        int comment '对端号码数',
  failure_cnt       int comment '发送失败次数'
) comment '短信下发统计日报'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\001' stored as textfile
;

--菜谱达人月度考核报表（2014版）
--菜谱 > 运营 > 菜谱达人月度考核报表（2014版）
drop table bing.rpt_rcp_vip_eval_mm_2014;
create table bing.rpt_rcp_vip_eval_mm_2014
(
  userid        string comment '好豆用户ID',
  username      string comment '用户名',
  vip_style     string comment '达人菜谱风格',
  login_cnt     int comment '登录次数',
  recipe_num    int comment '发布菜谱数',
  recipe_num_4s int comment '四星及以上菜谱数',
  photo_num     int comment '成果照数',
  comment_num   int comment '菜谱评论数',
  topic_num_5   int comment '乐在厨房话题数',
  topic_num_6   int comment '营养健康话题数',
  topic_num_8   int comment '厨房宝典话题数',
  last_act_day  string comment '最近发布日期'
) comment '菜谱达人月度考核报表（2014版）'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\001' stored as textfile
;

--菜谱达人月度考核报表
--菜谱 > 运营 > 菜谱达人月度考核报表
drop table bing.rpt_rcp_vip_eval_mm;
create table bing.rpt_rcp_vip_eval_mm
(
  userid        string comment '好豆用户ID',
  username      string comment '用户名',
  recipe_num    int comment '发布菜谱数',
  onlyrecipe_num    int comment '独家菜谱数',
  record_recipe_num int comment '三星收录菜谱数',
  recipe_num_4s     int comment '四星及以上菜谱数',
  comment_num   int comment '评论数。（菜谱，专辑，作品，小组）',
  topic_num_5   int comment '乐在厨房话题数（废）',
  topic_num_6   int comment '营养健康话题数',
  topic_num_8   int comment '厨房宝典话题数',
  good_topic_num    int comment '精华及推荐话题数',
  photo_num     int comment '作品数（手机端）',
  digg_daynum   int comment '点赞天数',
  dagg_num      int comment '点赞数',
  doubi_reward  int comment '奖励豆币',
  topic_num_31  int comment '享受餐桌话题数',
  topic_num_32  int comment '烘焙甜点话题数',
  topic_num_33  int comment '家常菜话题数'
) comment '菜谱达人月度考核报表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;


--菜谱专辑达人月度考核报表
--菜谱 > 运营 > 菜谱专辑达人月度考核报表
drop table bing.rpt_rcp_albumvip_eval_mm;
create table bing.rpt_rcp_albumvip_eval_mm
(
  userid        string comment '好豆用户ID',
  username      string comment '用户名',
  album_num     int comment '菜谱专辑数',
  staralbum_num int comment '4星及以上专辑数',
  comment_num   int comment '评论数。（菜谱，专辑，作品，小组）'
) comment '菜谱专辑达人月度考核报表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;

--广场小组组长月度考核报表
--菜谱 > 广场 > 广场小组组长月度考核报表
drop table bing.rpt_grp_admuser_eval_mm;
create table bing.rpt_grp_admuser_eval_mm
(
  userid              string comment '管理员用户ID',
  username            string comment '管理员名称',
  group_title         string comment '小组头衔',
  top_cnt             int comment '置顶',
  recom_cnt           int comment '推荐',
  digest_cnt          int comment '加精',
  move_cnt            int comment '转移',
  delete_cnt          int comment '删帖',
  edit_cnt            int comment '编辑话题数',
  reply_topic_num     int comment '回复话题数',
  eff_reply_topic_num int comment '有效回复话题数',
  comment_cnt         int comment '一级回复数',
  eff_comment_cnt     int comment '一级有效回复数',
  reply_cnt           int comment '所有回复数',
  present_cnt         int comment '赠送豆币时操作话题总数',
  present_wealth      int comment '赠送豆币总数',
  topic_num           int comment '发帖数',
  recomm_topic        int comment '被推荐帖数',
  digest_topic        int comment '被精华帖数',
  ash_topic_num       int comment '爱生活发贴数',
  qnc_topic_num       int comment '去哪吃发贴数',
  zms_topic_num       int comment '做美食发贴数'
) comment '广场小组组长月度考核报表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\001' stored as textfile
;

--广场小组月度考核报表
--http://jira.haodou.cn/browse/DATACENTER-333
drop table bing.rpt_grp_eval_mm;
create table bing.rpt_grp_eval_mm
(
  index_name        string comment '指标名。总计/日平均',
  group_id          string comment '小组ID，不分小组为*',
  group_name        string comment '小组名',
  topic_num         int comment '发帖数',
  topic_user        int comment '发帖人数',
  recomm_topic      int comment '推荐帖数',
  digest_topic      int comment '精华帖数',
  hot_topic         int comment '热门帖数',
  comment_num       int comment '回复数',
  comment_user      int comment '回复人数'
) comment '广场小组月度考核报表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;


--广场试用申请名单
drop table bing.rpt_grp_trialapply_info;
create table bing.rpt_grp_trialapply_info
(
  shoporderid   int comment '申请订单号',
  userid        string comment '好豆用户ID',
  username      string comment '用户名',
  buytime       timestamp comment '申请时间',
  realname      string comment '真实姓名',
  provincename  string comment '省份',
  cityname      string comment '城市',
  areaname      string comment '区域',
  address       string comment '地址',
  postcode      string comment '邮政编码',
  phone         string comment '电话',
  mobilephone   string comment '手机',
  remarks       string comment '申请说明',
  last_apply    string comment '最近申请',
  apply_cnt     int comment '申请次数',
  success_cnt   int comment '成功次数',
  apply_way     string comment '申请方式。web网站/app应用'
) comment '广场试用申请名单'
  partitioned by (shopgoodsid string)
  row format delimited fields terminated by '\001' stored as textfile
;

--广场小组周活跃度指标
drop table bing.rpt_grp_weekactrank_dm;
create table bing.rpt_grp_weekactrank_dm
(
  group_id         string comment '小组id',
  topic_num        int comment '话题发布数',
  topic_user       int comment '话题发布人数',
  startopic_num    int comment '推荐精华话题数',
  comment_num      int comment '话题评论数',
  comment_user     int comment '话题评论人数',
  commentnum_web   int comment 'Web话题评论数',
  commentnum_app   int comment 'App话题评论数',
  commentuser_web  int comment 'Web话题评论人数',
  commentuser_app  int comment 'App话题评论人数',
  score            int comment '本周活跃度得分',
  sn               int comment '本周活跃度排名'
) comment '广场小组周活跃度指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--广场成员周活跃度指标
drop table bing.rpt_grp_memberweekactrank_dm;
create table bing.rpt_grp_memberweekactrank_dm
(
  user_id          string comment '用户id',
  topic_num        int comment '话题发布数',
  startopic_num    int comment '推荐精华话题数',
  comment_num      int comment '话题评论数',
  commentnum_web   int comment 'Web话题评论数',
  commentnum_app   int comment 'App话题评论数',
  score            int comment '本周活跃度得分',
  sn               int comment '本周活跃度排名'
) comment '广场成员周活跃度指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

drop table bing.rpt_grp_memberweekactrank_dm_name;
create table bing.rpt_grp_memberweekactrank_dm_name
(
  user_id          string comment '用户id',
  user_name        string comment '用户名',
  topic_num        int comment '话题发布数',
  startopic_num    int comment '推荐精华话题数',
  comment_num      int comment '话题评论数',
  commentnum_web   int comment 'Web话题评论数',
  commentnum_app   int comment 'App话题评论数',
  score            int comment '本周活跃度得分',
  sn               int comment '本周活跃度排名'
) comment '广场成员周活跃度指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--达人/生活盟主周活跃度指标
drop table bing.rpt_grp_vipweekactrank_dm;
create table bing.rpt_grp_vipweekactrank_dm
(
  user_id          string comment '用户id',
  recipe_num        int comment '菜谱发布数',
  photo_num         int comment '作品发布数',
  topic_num        int comment '话题发布数',
  startopic_num    int comment '推荐精华话题数',
  comment_num      int comment '话题评论数',
  vip_score         int comment '达人活跃度得分。非达人为-1',
  life_score        int comment '生活盟主活跃度得分。非达人为-1',
  vip_sn            int comment '达人活跃度排名',
  life_sn           int comment '生活盟主活跃度排名'
) comment '达人/生活盟主周活跃度指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;


--安卓菜谱渠道监控指标
drop table bing.rpt_app_channel_index_dm;
create table bing.rpt_app_channel_index_dm
(
  app_id          string comment '好豆应用ID',
  channel_id      string comment '渠道ID。格式：渠道编码+版本。',
  version_id      string comment '版本ID',
  new_devnum      int comment '新增设备激活量',
  fake_devnum     int comment '虚假新增设备数',
  remain_basenum  int comment '留存设备基数（昨日新增设备激活量）',
  remain_devnum   int comment '留存设备数',
  remain_rate     float comment '次日留存率',
  act_devnum      int comment '活跃设备数',
  avg_dur         int comment '单次使用时长'
) comment '安卓菜谱渠道监控指标'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;

--//索引部分（IDX）//--
use bing;

create index idx_ods_qnc_citysite on table ods_qnc_citysite (cityid) 
  as 'org.apache.hadoop.hive.ql.index.compact.CompactIndexHandler' with deferred rebuild
  in table idx_ods_qnc_citysite
  location '/bing/idx/idx_ods_qnc_citysite';
alter index idx_ods_qnc_citysite on ods_qnc_citysite rebuild;


--//视频菜谱结果日表 add by wgzhao 2015-07-24//--
create table if not exists bing.rpt_app_video_dm (
    app_id string comment '应用ID',
    view_num int  comment '浏览数',
    dev_num int  comment '日活数',
    comment_num int  comment '评论数',
    fav_num int  comment '点赞数',
    collect_num int  comment '收藏数',
    shared_num int  comment '分享数',
    add_ratio float  comment '增长率'
)  comment '视频菜谱统计表--每日'
partitioned by (
    statis_date string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';

--//视频菜谱结果周表 add by wgzhao 2015-07-29//--
create table if not exists bing.rpt_app_video_wm (
    app_id string comment '应用ID',
    view_num int  comment '浏览数',
    dev_num int  comment '日活数',
    comment_num int  comment '评论数',
    fav_num int  comment '点赞数',
    collect_num int  comment '收藏数',
    shared_num int  comment '分享数',
    add_ratio float  comment '增长率'
)  comment '视频菜谱统计表--每周'
partitioned by (
    statis_week string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY '\t';



--应用API响应情况

drop table bing.rpt_app_apistat_dm;
create table bing.rpt_app_apistat_dm
(
  app_id          string comment '应用ID',
  method          string comment '方法',
  status          string comment 'HTTP状态',
  call_cnt        int comment '调用次数',
  dev_num         int comment '调用设备数'
) comment '应用API响应情况'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;


--CDN响应情况
drop table bing.rpt_haodou_cdnresp_dm;
create table bing.rpt_haodou_cdnresp_dm
(
  cdn             string comment 'CDN名称。目前包括：WS，DNION。',
  visitor_type    string comment '访客类型。分为：安卓菜谱、iPhone菜谱、安卓系统、其他。'
  res_type        string comment '资源类型。分为：文本（包括.js和.css）、图片（包括.png、.gif和.jpg）',
  status          string comment '响应状态。',
  call_cnt        int comment '调用次数',
  bytes           int comment '字节数（单位：MB）'
) comment 'CDN响应情况'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;



--应用页面访问详情
drop table bing.rpt_app_pagedetail_dm;
create table bing.rpt_app_pagedetail_dm
(
  app_id          string comment '好豆应用ID',
  version_id      string comment '版本ID',
  page_code       string comment '应用页面编码',
  page_name       string comment '应用页面名称',
  call_cnt        int comment '访问次数',
  dev_num         int comment '访问设备数'
) comment '应用功能调用详情'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;


--好豆菜谱标签构成周报
drop table bing.rpt_rcp_recipetag_wm;
create table bing.rpt_rcp_recipetag_wm
(
  tag_id           int comment '标签id',
  recipe_num       int comment '菜谱数',
  recipe_viewnum   int comment '菜谱浏览数',
  recipe_likenum   int comment '菜谱喜欢数'
) comment '好豆菜谱标签构成周报'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--好豆菜谱渠道IP TOP10报表
drop table bing.rpt_app_iptop10_ds;
create table bing.rpt_app_iptop10_ds
(
  app_id           string comment '应用id',
  channel_id       string comment '渠道id',
  userip           string comment 'ip',
  devnum           int comment '新增设备数'
) comment '好豆菜谱渠道IP TOP10报表'
  row format delimited fields terminated by '\t' stored as textfile
;


--好豆菜谱机型分布报表
drop table bing.rpt_app_devmodel_dm;
create table bing.rpt_app_devmodel_dm
(
  app_id       string comment '应用id',
  dev_brand    string comment '设备品牌',
  dev_model    string comment '设备型号',
  devnum       int comment '最近30天活跃设备数',
  newnum       int comment '最近30天新增设备数'
) comment '好豆菜谱机型分布报表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;



--好豆菜谱入网时长分布报表
drop table bing.rpt_app_indays_dist_dm;
create table bing.rpt_app_indays_dist_dm
(
  app_id       string comment '应用id',
  indays       int comment '入网时长。单位：天',
  call_cnt     int comment '访问次数',
  dev_num      int comment '访问设备数'
) comment '好豆菜谱入网时长分布报表'
  partitioned by (statis_date string)
  row format delimited fields terminated by '\t' stored as textfile
;



--好豆菜谱渠道IP集中情况
drop table bing.rpt_app_ipmass_ds;
create table bing.rpt_app_ipmass_ds
(
  app_id           string comment '应用id',
  channel_id       string comment '渠道id',
  devsum           int comment '新增设备总数',
  devs1            int comment '每ip新增设备<=1的新增设备总数',
  devs2            int comment '每ip新增设备<=2的新增设备总数',
  devs3            int comment '每ip新增设备<=3的新增设备总数',
  devs4            int comment '每ip新增设备<=4的新增设备总数',
  devs5            int comment '每ip新增设备<=5的新增设备总数',
  devs6            int comment '每ip新增设备<=6的新增设备总数',
  devs7            int comment '每ip新增设备<=7的新增设备总数',
  devs8            int comment '每ip新增设备<=8的新增设备总数',
  devs9            int comment '每ip新增设备<=9的新增设备总数',
  devs10           int comment '每ip新增设备<=10的新增设备总数',
  devs11           int comment '每ip新增设备<=11的新增设备总数',
  devsmore         int comment '每ip新增设备>=12的新增设备总数',
  devp1            float comment '每ip新增设备<=1的新增设备总数占比',
  devp2            float comment '每ip新增设备<=2的新增设备总数占比',
  devp3            float comment '每ip新增设备<=3的新增设备总数占比',
  devp4            float comment '每ip新增设备<=4的新增设备总数占比',
  devp5            float comment '每ip新增设备<=5的新增设备总数占比',
  devp6            float comment '每ip新增设备<=6的新增设备总数占比',
  devp7            float comment '每ip新增设备<=7的新增设备总数占比',
  devp8            float comment '每ip新增设备<=8的新增设备总数占比',
  devp9            float comment '每ip新增设备<=9的新增设备总数占比',
  devp10           float comment '每ip新增设备<=10的新增设备总数占比',
  devp11           float comment '每ip新增设备<=11的新增设备总数占比',
  devpmore         float comment '每ip新增设备>=12的新增设备总数占比'
) comment '好豆菜谱渠道IP集中情况'
  row format delimited fields terminated by '\t' stored as textfile
;


--管理员豆币发放报表
drop table bing.rpt_haodou_admin_doubi_mm;
create table bing.rpt_haodou_admin_doubi_mm
(
  operid       string comment '管理员id',
  opername     string comment '管理员姓名',
  usercnt      int comment '累计发送人次',
  doubi        int comment '累计发送豆币'
) comment '管理员豆币发放报表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;



