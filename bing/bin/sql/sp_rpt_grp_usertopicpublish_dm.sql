
--小组用户的每天发帖情况
insert overwrite table bing.rpt_grp_usertopicpublish_dm partition (statis_month='${firstday_date}')
select userid, to_date(createtime) as createdate, 
count(topicid) as topicnum,
count(case when cateid in (33,32,31,8,6) then topicid end) as zmsnum,
count(case when cateid in (34,35,23,38,30,28) then topicid end) as ashnum,
count(case when recommend=1 or digest=1 then topicid end) as topnum,
day(to_date(createtime)) as createday,
row_number() over (partition by userid order by to_date(createtime)) as sn
from haodou_center_${curdate}.GroupTopic
where createtime between '${firstday_date} 00:00:00' and '${lastday_date} 23:59:59' and status=1
and cateid in (33,32,31,8,6,34,35,23,38,30,28)
group by userid, to_date(createtime)
;

--当月小组用户连续发布话题情况
insert overwrite table bing.rpt_grp_persistpublish_mm partition (statis_month='${firstday_date}')
select t1.userid, u.username, t1.topicnums, t1.zmsnums, t1.ashnums, t1.topnums, t1.pdays, t1.daylist
from (select userid, offset, max(createday)-min(createday)+1 as pdays,
sum(topicnum) as topicnums,
sum(zmsnum) as zmsnums,
sum(ashnum) as ashnums,
sum(topnum) as topnums,
concat_ws(', ',collect_set(createdate)) as daylist,
row_number() over (partition by userid order by max(createday)-min(createday) desc) as sn
from (select userid, createdate, topicnum, zmsnum, ashnum, topnum,
createday, createday-sn as offset
from bing.rpt_grp_usertopicpublish_dm
where statis_month='${firstday_date}'
) t0
group by userid, offset
) t1 inner join haodou_passport_${curdate}.`user` u on (t1.userid=u.userid)
where t1.sn=1 and t1.pdays>1
;
