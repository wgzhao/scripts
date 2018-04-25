
insert overwrite table bing.rpt_rcp_wealthbill_dm partition(ptdate='${statis_date}')
select type, wealthtitle, count(1) cnt, count(distinct userid) user_cnt, sum(wealth) wealth_sum
from haodou_center_${curdate}.UserWealthLog 
where to_date(createtime) = '${statis_date}'
group by type, wealthtitle;
