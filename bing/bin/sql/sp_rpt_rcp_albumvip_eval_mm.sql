
--菜谱专辑达人月度考核报表

--创建临时表
create table if not exists bing.tmp_rcp_albumvip_eval_mm
(
  userid        string comment '好豆用户ID',
  username      string comment '用户名',
  album_num     int comment '菜谱专辑数',
  staralbum_num int comment '4星及以上专辑数',
  comment_num   int comment '评论数。（菜谱，专辑，作品，小组）'
) comment '菜谱专辑达人月度考核报表临时表'
  partitioned by (statis_month string)
  row format delimited fields terminated by '\t' stored as textfile
;

--初始化
insert overwrite table bing.tmp_rcp_albumvip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v,tg)*/ v.userid, v.username, 0, 0, 0
from haodou_center_${curdate}.VipUser v 
left semi join
(select userid from haodou_center_${curdate}.VipUserTag where tagid=10038) tg
on (v.userid=tg.userid)
where v.viptype='1' and v.status='1'
;

--  album_num     int comment '菜谱专辑数',
--  staralbum_num int comment '4星及以上专辑数',
insert into table bing.tmp_rcp_albumvip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ aa.userid, '' as username, 
count(aa.AlbumId) as album_num, 
count(case when aa.Rate>=4 then aa.AlbumId end) as staralbum_num, 
0
from haodou_recipe_${curdate}.Album aa left semi join
(select userid from bing.tmp_rcp_albumvip_eval_mm where statis_month='${statis_month}') v on (aa.userid=v.userid)
where aa.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and aa.sys=0 and aa.status=1
group by aa.userid
;

--  comment_num   int comment '评论数。（菜谱，专辑，作品，小组）'
insert into table bing.tmp_rcp_albumvip_eval_mm partition (statis_month='${statis_month}')
select /*+ mapjoin(v)*/ cc.userid, '' as username, 
0, 0, 
count(cc.commentid) as comment_num
from haodou_comment_${curdate}.Comment cc left semi join
(select userid from bing.tmp_rcp_albumvip_eval_mm where statis_month='${statis_month}') v on (cc.userid=v.userid)
where cc.createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59'
and cc.status=1
group by cc.userid
;


--写入结果表
insert overwrite table bing.rpt_rcp_albumvip_eval_mm partition (statis_month='${statis_month}')
select userid, 
max(username), 
sum(album_num),
sum(staralbum_num),
sum(comment_num)
from bing.tmp_rcp_albumvip_eval_mm
where statis_month='${statis_month}'
group by userid
;
