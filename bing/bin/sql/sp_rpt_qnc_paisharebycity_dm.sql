insert overwrite table bing.rpt_qnc_paisharebycity_dm partition(ptdate='${statisdate}')
select 
if(isnull(t1.cityid),t2.cityid,t1.cityid) cityid,
if(isnull(t1.verified),0, t1.verified) verified_count,
if(isnull(t1.disverified),0, t1.disverified) disverified_count,
if(isnull(t2.user_count),0, t2.user_count) user_count
from(
select cityid, 
sum(if(status=1,1,0)) verified, 
sum(if(status=2,1,0)) disverified
from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) ='${statis_date}' group by cityid
) t1 full outer join(
select cityid, count(1) count, 
count(distinct userid) user_count
from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) ='${statis_date}' and status=1 group by cityid
) t2 
on t1.cityid = t2.cityid;
