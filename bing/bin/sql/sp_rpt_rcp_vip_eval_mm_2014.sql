
--菜谱达人月度考核报表

--创建临时表
create table if not exists bing.tmp_rcp_vip_eval_mm_2014
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
) comment '菜谱达人月度考核报表临时表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\001' stored as textfile
;

--初始化
insert overwrite table bing.tmp_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select userid, username, expert_style, 0, 0, 0, 0, 0, 0, 0, 0, '0000-00-00'
from bing.ods_rcp_expert
where expert_type='个人' and expert_level='菜谱达人' and status=1
;

--签到数
insert into table bing.tmp_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/
ll.userid, '', '', count(distinct createdatetime), 0, 0, 0, 0, 0, 0, 0, max(to_date(ll.createdatetime))
from haodou_checkin_${curdate}.CheckInLog ll inner join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (ll.userid=v.userid)
where ll.createdatetime between '${firstday_date}' and '${lastday_date}'
group by ll.userid
;

--菜谱数/四星及以上菜谱数
insert into table bing.tmp_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/
rr.userid, '', '', 0, count(rr.recipeid), count(case when rr.rate >= 4 then rr.recipeid end), 0, 0, 0, 0, 0, 
max(to_date(coalesce(rr.updatetime,rr.createtime)))
from haodou_recipe_${curdate}.Recipe rr inner join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (rr.userid=v.userid)
where coalesce(rr.reviewtime,rr.updatetime,rr.createtime) between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and rr.status in ('0','10')
group by rr.userid
;

--成果照
insert into table bing.tmp_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/
pp.userid, '', '', 0, 0, 0, count(pp.id), 0, 0, 0, 0, max(to_date(pp.createtime))
from haodou_photo_${curdate}.Photo pp inner join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (pp.userid=v.userid)
where pp.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and pp.status='1'
group by pp.userid
;

--菜谱评论数
insert into table bing.tmp_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/
cc.userid, '', '', 0, 0, 0, 0, count(cc.commentid), 0, 0, 0, max(to_date(cc.createtime))
from haodou_comment_${curdate}.Comment cc inner join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (cc.userid=v.userid)
where cc.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and cc.type='0' and cc.status='1'
group by cc.userid
;

--乐在厨房话题数/营养健康话题数/厨房宝典话题数
insert into table bing.tmp_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/
tt.userid, '', '', 0, 0, 0, 0, 0, 
count(case when tt.cateid='5' then tt.topicid end), 
count(case when tt.cateid='6' then tt.topicid end), 
count(case when tt.cateid='8' then tt.topicid end), 
max(to_date(tt.createtime))
from haodou_center_${curdate}.GroupTopic tt inner join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (tt.userid=v.userid)
where tt.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and tt.cateid in ('5','6','8') and tt.status='1'
group by tt.userid
;

--写入结果表
insert overwrite table bing.rpt_rcp_vip_eval_mm_2014 partition (statis_month='${statis_month}')
select userid, 
max(username     ),
max(vip_style    ),
sum(login_cnt    ),
sum(recipe_num   ),
sum(recipe_num_4s),
sum(photo_num    ),
sum(comment_num  ),
sum(topic_num_5  ),
sum(topic_num_6  ),
sum(topic_num_8  ),
max(last_act_day )
from bing.tmp_rcp_vip_eval_mm_2014
where statis_month='${statis_month}'
group by userid
;
