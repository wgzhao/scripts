
add jar hdfs://hdcluster/udf/bing.jar;
create temporary function html2text as 'com.haodou.hive.bing.Html2Text';

--广场小组组长月度考核报表

--创建临时表
create table if not exists bing.tmp_grp_admuser_eval_mm
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
) comment '广场小组组长月度考核报表临时表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\001' stored as textfile
;

--初始化
insert overwrite table bing.tmp_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select userid, username, concat(group_name,group_title), 
0 as top_cnt, 0 as recom_cnt, 0 as digest_cnt, 0 as move_cnt, 0 as delete_cnt, 
0 as edit_cnt, 
0 as reply_topic_num, 0 as eff_reply_topic_num, 0 as comment_cnt, 0 as eff_comment_cnt, 0 as reply_cnt, 
0 as present_cnt, 0 as present_wealth, 
0 as topic_num, 0 as recomm_topic, 0 as digest_topic, 0 as ash_topic_num, 0 as qnc_topic_num, 0 as zms_topic_num      
from bing.ods_grp_admuser
where status=1 and eff_date<='${statis_date}'
;

--置顶/推荐/加精/转移/删帖
insert into table bing.tmp_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(gu)*/
ll.userid, '', '', 
count(case when ll.type=3 and ll.num=1 then ll.grouplogid end) as top_cnt,
count(case when ll.type=2 and ll.num=1 then ll.grouplogid end) as recom_cnt,
count(case when ll.type=1 and ll.num=1 then ll.grouplogid end) as digest_cnt,
count(case when ll.type=7 and ll.num=0 then ll.grouplogid end) as move_cnt,
count(case when ll.type=6 and ll.num=0 then ll.grouplogid end) as delete_cnt,
0 as edit_cnt, 
0 as reply_topic_num, 0 as eff_reply_topic_num, 0 as comment_cnt, 0 as eff_comment_cnt, 0 as reply_cnt, 
0 as present_cnt, 0 as present_wealth, 
0 as topic_num, 0 as recomm_topic, 0 as digest_topic, 0 as ash_topic_num, 0 as qnc_topic_num, 0 as zms_topic_num      
from haodou_center_${curdate}.GroupLog ll
left semi join bing.ods_grp_admuser gu on (ll.userid=gu.userid and gu.status=1)
where ll.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
group by ll.userid
;

--编辑话题数
insert into table bing.tmp_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(gu)*/
tt.updateuserid, '', '',
0 as top_cnt, 0 as recom_cnt, 0 as digest_cnt, 0 as move_cnt, 0 as delete_cnt, 
count(tt.topicid) as edit_cnt,
0 as reply_topic_num, 0 as eff_reply_topic_num, 0 as comment_cnt, 0 as eff_comment_cnt, 0 as reply_cnt, 
0 as present_cnt, 0 as present_wealth, 
0 as topic_num, 0 as recomm_topic, 0 as digest_topic, 0 as ash_topic_num, 0 as qnc_topic_num, 0 as zms_topic_num      
from haodou_center_${curdate}.GroupTopic tt
left semi join bing.ods_grp_admuser gu on (tt.updateuserid=gu.userid and gu.status=1)
where tt.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and tt.title not like '%抢楼%' and tt.status=1
and tt.userid!=tt.updateuserid
group by tt.updateuserid
;

--回复话题数/有效回复话题数/一级回复数/一级有效回复数/所有回复数
insert into table bing.tmp_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(gu)*/
cc.userid, '', '',
0 as top_cnt, 0 as recom_cnt, 0 as digest_cnt, 0 as move_cnt, 0 as delete_cnt, 
0 as edit_cnt, 
count(distinct cc.itemid) as reply_topic_num,
count(distinct case when length(html2text(cc.content)) > 10 then cc.itemid end) as eff_reply_topic_num,
count(case when cc.replyid=0 then cc.commentid end) as comment_cnt,
count(case when cc.replyid=0 and length(html2text(cc.content)) > 10 then cc.commentid end) as eff_comment_cnt,
count(cc.commentid) as reply_cnt,
0 as present_cnt, 0 as present_wealth, 
0 as topic_num, 0 as recomm_topic, 0 as digest_topic, 0 as ash_topic_num, 0 as qnc_topic_num, 0 as zms_topic_num      
from haodou_comment_${curdate}.Comment cc
left semi join bing.ods_grp_admuser gu on (cc.userid=gu.userid and gu.status=1)
where cc.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and cc.type=6 and cc.status=1
group by cc.userid
;

--赠送豆币时操作话题总数/赠送豆币总数
insert into table bing.tmp_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(gu)*/
wl.userid, '', '',
0 as top_cnt, 0 as recom_cnt, 0 as digest_cnt, 0 as move_cnt, 0 as delete_cnt, 
0 as edit_cnt, 
0 as reply_topic_num, 0 as eff_reply_topic_num, 0 as comment_cnt, 0 as eff_comment_cnt, 0 as reply_cnt, 
count(wl.integralid) as present_cnt,
abs(sum(wl.wealth)) as present_wealth,
0 as topic_num, 0 as recomm_topic, 0 as digest_topic, 0 as ash_topic_num, 0 as qnc_topic_num, 0 as zms_topic_num      
from haodou_center_${curdate}.UserWealthLog wl
left semi join bing.ods_grp_admuser gu on (wl.userid=gu.userid and gu.status=1)
where wl.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and wl.type=0 and wl.wealthtitle='话题送豆币扣除' and wl.wealth<0
group by wl.userid
;

--发帖数/被推荐帖数/被精华帖数/爱生活发贴数/去哪吃发贴数/做美食发贴数
insert into table bing.tmp_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(gu)*/
tt.userid, '', '',
0 as top_cnt, 0 as recom_cnt, 0 as digest_cnt, 0 as move_cnt, 0 as delete_cnt, 
0 as edit_cnt, 
0 as reply_topic_num, 0 as eff_reply_topic_num, 0 as comment_cnt, 0 as eff_comment_cnt, 0 as reply_cnt, 
0 as present_cnt, 0 as present_wealth, 
count(tt.topicid) as topic_num,
count(case when tt.recommend=1 then tt.topicid end) as recomm_topic,
count(case when tt.digest=1 then tt.topicid end) as digest_topic,
count(case when tt.cateid in (34,35,23,30,28,29,38) then tt.topicid end) as ash_topic_num,
0 as qnc_topic_num,
count(case when tt.cateid in (33,32,31,8,6) then tt.topicid end) as zms_topic_num
from haodou_center_${curdate}.GroupTopic tt
left semi join bing.ods_grp_admuser gu on (tt.userid=gu.userid and gu.status=1)
where tt.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and tt.title not like '%抢楼%' and tt.status=1
group by tt.userid
;

--写入结果表
insert overwrite table bing.rpt_grp_admuser_eval_mm partition (statis_month='${statis_month}')
select userid, 
max(username),
max(group_title),
sum(top_cnt            ),
sum(recom_cnt          ),
sum(digest_cnt         ),
sum(move_cnt           ),
sum(delete_cnt         ),
sum(edit_cnt           ),
sum(reply_topic_num    ),
sum(eff_reply_topic_num),
sum(comment_cnt        ),
sum(eff_comment_cnt    ),
sum(reply_cnt          ),
sum(present_cnt        ),
sum(present_wealth     ),
sum(topic_num          ),
sum(recomm_topic       ),
sum(digest_topic       ),
sum(ash_topic_num      ),
sum(qnc_topic_num      ),
sum(zms_topic_num      )
from bing.tmp_grp_admuser_eval_mm
where statis_month='${statis_month}'
group by userid
;
