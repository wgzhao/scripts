insert overwrite table bing.rpt_qnc_signindist_mm partition(ptdate='${statisdate}')
select userid, count(distinct to_date(signtime)) count
from qnc_haodou_center_${curdate}.usersignlog
where to_date(signtime) between '${preday30_date}' and '${statis_date}'
group by userid;
