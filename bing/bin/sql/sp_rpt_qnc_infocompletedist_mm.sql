insert overwrite table bing.rpt_qnc_infocompletedist_mm partition(ptdate='${statisdate}')
select count(1) count,
sum(if(qnc_u.avatar!=0,1,0)) with_photo,
sum(if(qnc_u.gender!=2,1,0)) with_gender,
sum(if(qnc_u.birthday != 'NULL' and qnc_u.birthday != '',1,0)) with_birthday,
sum(if(qnc_u.cityid !=0,1,0)) with_city,
sum(if(qnc_u.intro!='NULL' and qnc_u.intro != '',1,0)) with_intro
from haodou_passport_${curdate}.user u
join
qnc_qunachi_user_${curdate}.user qnc_u
on u.userid = qnc_u.userid
where to_date(u.regtime) between '${preday30_date}' and '${statis_date}'
and u.status=1;
