insert overwrite table bing.rpt_qnc_paishare_dm partition(ptdate='${statisdate}')
select v3.verified, v3.disverified,  
if(isnull(v1.web_count), 0, v1.web_count) web_count,
if(isnull(v1.web_user_count), 0, v1.web_user_count) web_user_count,
if(isnull(v1.android_count), 0, v1.android_count) android_count,
if(isnull(v1.android_user_count), 0, v1.android_user_count) android_user_count,
if(isnull(v1.ios_count), 0, v1.ios_count) ios_count,
if(isnull(v1.ios_user_count), 0, v1.ios_user_count) ios_user_count,
v2.*,
if(isnull(v1.with_price), 0, v1.with_price) with_price,
if(isnull(v1.with_content), 0, v1.with_content) with_content,
if(isnull(v1.with_shop), 0, v1.with_shop) with_shop
from(
select 
sum(if(platform=0,count,0)) web_count,
sum(if(platform=0,user_count,0)) web_user_count,
sum(if(platform=1,count,0)) android_count,
sum(if(platform=1,user_count,0)) android_user_count,
sum(if(platform=2,count,0)) ios_count,
sum(if(platform=2,user_count,0)) ios_user_count,
sum(with_price) with_price,
sum(with_content) with_content,
sum(with_shop) with_shop
from
(select 
`from` platform,
count(1) count, 
count(distinct userid) user_count,
sum(if(price=0.0,1,0)) with_price, 
sum(if(length(content)!=0,1,0)) with_content,
sum(if(shopid!=0,1,0)) with_shop 
from qnc_haodou_pai_${curdate}.paishare 
where to_date(createtime) ='${statis_date}' and status=1
group by `from`) t) v1
full outer join
(select t1.web_and_android, t2.web_and_ios
from
(select count(ps1.userid) web_and_android
from 
(select distinct userid from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) = '${statis_date}' and `from` = 0 and status=1) ps1
join 
(select distinct userid from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) = '${statis_date}' and `from` = 1 and status=1) ps2
on ps1.userid = ps2.userid) t1
full outer join
(select count(ps1.userid) web_and_ios
from 
(select distinct userid from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) = '${statis_date}' and `from` = 0 and status=1) ps1
join 
(select distinct userid from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) = '${statis_date}' and `from` = 2 and status=1) ps2
on ps1.userid = ps2.userid) t2) v2
full outer join
(select sum(if(status=1,1,0)) verified,sum(if(status=0,1,0)) disverified from qnc_haodou_pai_${curdate}.paishare where to_date(createtime) ='${statis_date}') v3;
