
insert overwrite table bing.rpt_hoto_reguser_dm partition (statis_date='${statis_date}')
select 
count(userid) as usernum,
count(case when regtime >= '${statis_date} 00:00:00' then userid end) as newuser
from haodou_passport_${curdate}.`User`
where regtime <= '${statis_date} 23:59:59'
;
