
create table if not exists bing.tmp_grp_avgtopicomment_wm
(
  group_id          string comment '小组ID，不分小组为*',
  topic_num         int comment '发帖数',
  topic_user        int comment '发帖人数',
  comment_num       int comment '回复数',
  comment_user      int comment '回复人数'
) comment '好豆BI周报结果表7-当周日平均发帖回复临时表'
  partitioned by (statis_week string)
  row format delimited fields terminated by '\t' stored as textfile
;

--广场发帖
insert overwrite table bing.tmp_grp_avgtopicomment_wm partition (statis_week='${statisweek_firstday}')
select '*' as group_id,
round(avg(topic_num)) as topic_num,
round(avg(user_num)) as topic_user,
0 as comment_num,
0 as comment_user
from (
select to_date(createtime) as statis_date,
count(topicid) as topic_num,
count(distinct userid) as user_num
from haodou_center_${curdate}.GroupTopic
where createtime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and status='1' and cateid in ('5','6','8','2','3','24','25','12','11','10','23','27')
group by to_date(createtime)
) t
;

--广场回复
insert into table bing.tmp_grp_avgtopicomment_wm partition (statis_week='${statisweek_firstday}')
select '*' as group_id,
0 as topic_num,
0 as topic_user,
round(avg(comment_num)) as comment_num,
round(avg(user_num)) as comment_user
from (
select uc.statis_date,
count(uc.commentid) as comment_num,
count(distinct uc.userid) as user_num
from (select to_date(createtime) as statis_date, userid, itemid as topicid, commentid
from haodou_comment_${curdate}.Comment
where createtime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and type = '6' and status = '1'
) uc inner join
haodou_center_${curdate}.GroupTopic ut on (uc.topicid=ut.topicid)
where ut.cateid in ('5','6','8','2','3','24','25','12','11','10','23','27')
group by uc.statis_date
) t
;

--写入结果表
insert overwrite table bing.rpt_grp_avgtopicomment_wm partition (statis_week='${statisweek_firstday}')
select '*' as group_id,
sum(topic_num),
sum(topic_user),
sum(comment_num),
sum(comment_user)
from bing.tmp_grp_avgtopicomment_wm
where statis_week='${statisweek_firstday}'
;
