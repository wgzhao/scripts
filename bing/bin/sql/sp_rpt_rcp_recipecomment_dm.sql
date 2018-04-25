
--菜谱评论日指标

--更新最近7天
insert overwrite table bing.rpt_rcp_recipecomment_dm
select *
from bing.rpt_rcp_recipecomment_dm
where statis_date < '${preday7_date}'
;

insert into table bing.rpt_rcp_recipecomment_dm
select
to_date(cm.createtime) as statis_date,
count(case when cm.replyid=0 then cm.commentid end) as comment_num,
count(distinct case when cm.replyid=0 then cm.userid end) as user_num,
count(distinct case when cm.replyid=0 then cm.itemid end) as recipe_num,
count(case when cm.replyid=0 and cm.platform=0 then cm.commentid end) as web_commentnum,
count(case when cm.replyid=0 and cm.platform=1 then cm.commentid end) as android_commentnum,
count(case when cm.replyid=0 and cm.platform=2 then cm.commentid end) as iphone_commentnum,
count(distinct case when cm.replyid=0 and cm.platform=0 then cm.userid end) as web_usernum,
count(distinct case when cm.replyid=0 and cm.platform=1 then cm.userid end) as android_usernum,
count(distinct case when cm.replyid=0 and cm.platform=2 then cm.userid end) as iphone_usernum,
count(case when cm.replyid!=0 then cm.commentid end) as reply_num,
count(distinct case when cm.replyid!=0 then cm.userid end) as reply_usernum
from haodou_comment_${curdate}.Comment cm
where cm.createtime between '${preday7_date} 00:00:00' and '${statis_date} 23:59:59'
and cm.status=1 and cm.type=0
group by to_date(cm.createtime)
;
