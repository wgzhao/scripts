
--菜谱达人月度考核报表

--创建临时表
create table if not exists bing.tmp_rcp_vip_eval_mm
(
  userid        string comment '好豆用户ID',
  username      string comment '用户名',
  recipe_num    int comment '发布菜谱数',
  onlyrecipe_num    int comment '独家菜谱数',
  record_recipe_num int comment '三星收录菜谱数',
  recipe_num_4s     int comment '四星及以上菜谱数',
  comment_num   int comment '评论数。（菜谱，专辑，作品，小组）',
  topic_num_5   int comment '乐在厨房话题数',
  topic_num_6   int comment '营养健康话题数',
  topic_num_8   int comment '厨房宝典话题数',
  good_topic_num    int comment '精华及推荐话题数',
  photo_num     int comment '作品数（手机端）',
  digg_daynum   int comment '点赞天数',
  dagg_num      int comment '点赞数',
  doubi_reward  int comment '奖励豆币'
) comment '菜谱达人月度考核报表临时表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;

--初始化
insert overwrite table bing.tmp_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select userid, username, 
0 as recipe_num,
0 as onlyrecipe_num,
0 as record_recipe_num,
0 as recipe_num_4s,
0 as comment_num,
0 as topic_num_5,
0 as topic_num_6,
0 as topic_num_8,
0 as good_topic_num,
0 as photo_num,
0 as digg_daynum,
0 as dagg_num,
0 as doubi_reward
from bing.ods_rcp_expert
where expert_type='个人' and expert_level='菜谱达人' and status=1
;

--  recipe_num    int comment '发布菜谱数',
--  onlyrecipe_num    int comment '独家菜谱数',
--  record_recipe_num int comment '三星收录菜谱数',
--  recipe_num_4s     int comment '四星及以上菜谱数',
insert into table bing.tmp_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ rr.userid, '' as username, 
count(rr.recipeid) as recipe_num,
count(case when rr.Exclusive=1 then rr.recipeid end) as onlyrecipe_num,
count(case when rr.rate=3 and rr.Record=1 then rr.recipeid end) as record_recipe_num,
count(case when rr.rate>=4 then rr.recipeid end) as recipe_num_4s,
0 as comment_num,
0 as topic_num_5,
0 as topic_num_6,
0 as topic_num_8,
0 as good_topic_num,
0 as photo_num,
0 as digg_daynum,
0 as dagg_num,
0 as doubi_reward
from haodou_recipe_${curdate}.Recipe rr left semi join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (rr.userid=v.userid)
where rr.ReviewTime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and rr.status=0
group by rr.userid
;

--  comment_num   int comment '评论数。（菜谱，专辑，作品，小组）',
-- 0菜谱，1专辑，12作品，6小组
insert into table bing.tmp_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ cc.userid, '' as username, 
0 as recipe_num,
0 as onlyrecipe_num,
0 as record_recipe_num,
0 as recipe_num_4s,
count(cc.commentid) as comment_num,
0 as topic_num_5,
0 as topic_num_6,
0 as topic_num_8,
0 as good_topic_num,
0 as photo_num,
0 as digg_daynum,
0 as dagg_num,
0 as doubi_reward
from haodou_comment_${curdate}.Comment cc left semi join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (cc.userid=v.userid)
where cc.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and cc.status=1
group by cc.userid
;

--  topic_num_5   int comment '乐在厨房话题数',
--  topic_num_6   int comment '营养健康话题数',
--  topic_num_8   int comment '厨房宝典话题数',
--  good_topic_num    int comment '精华及推荐话题数',
insert into table bing.tmp_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ tt.userid, '' as username, 
0 as recipe_num,
0 as onlyrecipe_num,
0 as record_recipe_num,
0 as recipe_num_4s,
0 as comment_num,
count(case when tt.cateid=5 then tt.topicid end) as topic_num_5,
count(case when tt.cateid=6 then tt.topicid end) as topic_num_6,
count(case when tt.cateid=8 then tt.topicid end) as topic_num_8,
count(case when tt.Digest=1 then tt.topicid when tt.Recommend=1 then tt.topicid end) as good_topic_num,
0 as photo_num,
0 as digg_daynum,
0 as dagg_num,
0 as doubi_reward
from haodou_center_${curdate}.GroupTopic tt left semi join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (tt.userid=v.userid)
where tt.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and tt.cateid in (5,6,8) and tt.status=1
group by tt.userid
;

--  photo_num     int comment '作品数（手机端）',
insert into table bing.tmp_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ pp.userid, '' as username, 
0 as recipe_num,
0 as onlyrecipe_num,
0 as record_recipe_num,
0 as recipe_num_4s,
0 as comment_num,
0 as topic_num_5,
0 as topic_num_6,
0 as topic_num_8,
0 as good_topic_num,
count(pp.id) as photo_num,
0 as digg_daynum,
0 as dagg_num,
0 as doubi_reward
from haodou_photo_${curdate}.Photo pp left semi join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (pp.userid=v.userid)
where pp.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and pp.`From`!=0 and pp.status=1
group by pp.userid
;

--  digg_daynum   int comment '点赞天数',
--  dagg_num      int comment '点赞数'
insert into table bing.tmp_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ dd.userid, '' as username, 
0 as recipe_num,
0 as onlyrecipe_num,
0 as record_recipe_num,
0 as recipe_num_4s,
0 as comment_num,
0 as topic_num_5,
0 as topic_num_6,
0 as topic_num_8,
0 as good_topic_num,
0 as photo_num,
count(distinct to_date(dd.createtime)) as digg_daynum,
count(dd.id) as dagg_num,
0 as doubi_reward
from haodou_digg_${curdate}.Digg dd left semi join
(select userid from bing.ods_rcp_expert where expert_type='个人' and expert_level='菜谱达人' and status=1) v on (dd.userid=v.userid)
where dd.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
group by dd.userid
;

--写入结果表
insert overwrite table bing.rpt_rcp_vip_eval_mm partition (statis_month='${statis_month}')
select userid, 
max(username), 
sum(recipe_num),
sum(onlyrecipe_num),
sum(record_recipe_num),
sum(recipe_num_4s),
sum(comment_num),
sum(topic_num_5),
sum(topic_num_6),
sum(topic_num_8),
sum(good_topic_num),
sum(photo_num),
sum(digg_daynum),
sum(dagg_num),
case 
when (sum(topic_num_5)+sum(topic_num_6)+sum(topic_num_8))>=1 and sum(comment_num)>=50 and sum(recipe_num_4s)>=10 then 1500
when (sum(topic_num_5)+sum(topic_num_6)+sum(topic_num_8))>=1 and sum(comment_num)>=50 and (sum(record_recipe_num)+sum(recipe_num_4s))>=10 then 1200
when (sum(topic_num_5)+sum(topic_num_6)+sum(topic_num_8))>=1 and sum(comment_num)>=50 and (sum(record_recipe_num)+sum(recipe_num_4s))>=5 then 800
else 0 end as doubi_reward
from bing.tmp_rcp_vip_eval_mm
where statis_month='${statis_month}'
group by userid
;
