
--广场小组月度考核报表
--http://jira.haodou.cn/browse/DATACENTER-333

--创建临时表
create table if not exists bing.tmp_rpt_grp_eval_mm
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

--话题发布数 话题发布人数  推荐话题数 精华话题数 热门话题数
insert overwrite table bing.tmp_rpt_grp_eval_mm partition (statis_month='${statis_month}')
select '总计' as index_name,
coalesce(cateid,'*') as group_id, 
'' as group_name,
count(topicid) as topic_num,
count(distinct userid) as topic_user,
count(case when recommend=1 then topicid end) as recomm_topic,
count(case when digest=1 then topicid end) as digest_topic,
count(case when commentcount>=20 and recommend!=1 and digest!=1 then topicid end) as hot_topic,
0 as comment_num, 
0 as comment_user
from haodou_center_${curdate}.GroupTopic tt
where createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and cateid not in (16,17,26,28) and status=1  --排除16:站务帮助/17:公告/26:生活品牌馆/28:豆友会
group by cateid with rollup
;

--评论数 评论人数
insert into table bing.tmp_rpt_grp_eval_mm partition (statis_month='${statis_month}')
select '总计' as index_name,
coalesce(ut.cateid,'*') as group_id,
'' as group_name,
0 as topic_num,
0 as topic_user,
0 as recomm_topic,
0 as digest_topic,
0 as hot_topic,
count(uc.commentid) as comment_num,
count(distinct uc.userid) as user_num
from 
(select commentid, userid, itemid as topicid
from haodou_comment_${curdate}.Comment
where createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and type = '6' and status = '1'
) uc inner join
haodou_center_${curdate}.GroupTopic ut on (uc.topicid=ut.topicid)
where ut.cateid not in (16,17,26,28)  --排除16:站务帮助/17:公告/26:生活品牌馆/28:豆友会
group by ut.cateid with rollup
;

--写入结果表
insert overwrite table bing.rpt_grp_eval_mm partition (statis_month='${statis_month}')
select 
t.index_name, 
t.group_id,
case when t.group_id='*' then '所有小组' else coalesce(g.`name`,concat('小组',t.group_id)) end,
t.topic_num   ,
t.topic_user  ,
t.recomm_topic,
t.digest_topic,
t.hot_topic   ,
t.comment_num ,
t.comment_user
from (select index_name, group_id,
sum(topic_num   ) as topic_num   ,
sum(topic_user  ) as topic_user  ,
sum(recomm_topic) as recomm_topic,
sum(digest_topic) as digest_topic,
sum(hot_topic   ) as hot_topic   ,
sum(comment_num ) as comment_num ,
sum(comment_user) as comment_user
from bing.tmp_rpt_grp_eval_mm
where statis_month='${statis_month}'
group by index_name, group_id
) t left outer join
haodou_center_${curdate}.GroupCate g on (t.group_id=g.cateid)
;
