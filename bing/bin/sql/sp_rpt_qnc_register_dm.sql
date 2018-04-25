insert overwrite table bing.rpt_qnc_register_dm partition(ptdate='${statisdate}')
select count(1) total,
sum(if(p.fromplatform=0,1,0)) web,
sum(if(p.fromplatform=1,1,0)) android,
sum(if(p.fromplatform=2,1,0)) ios,
sum(if(p.fromplatform=3,1,0)) mobile_web,
sum(if(p.fromplatform=4,1,0)) win_8,
sum(if(p.fromplatform=5,1,0)) android_pad,
sum(if(p.fromplatform=6,1,0)) ios_pad,
sum(if(p.fromway=0,1,0)) email,
sum(if(p.fromway=1,1,0)) phone,
sum(if(p.fromway=20,1,0)) sina_weibo,
sum(if(p.fromway=21,1,0)) qq_weibo, 
sum(if(p.fromway=22,1,0)) qq,
sum(if(p.fromway=23,1,0)) douban,
sum(if(p.fromway=24,1,0)) wy163,
sum(if(p.fromway=25,1,0)) taobao
from haodou_passport_${curdate}.user p
join qnc_qunachi_user_${curdate}.user u
on p.userid = u.userid
where to_date(p.regtime) = '${statis_date}'
and to_date(u.createtime) = '${statis_date}';
