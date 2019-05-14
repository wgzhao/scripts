insert overwrite table bing.rpt_qnc_followdist_mm partition(ptdate='${statisdate}')
select userid, count(1) count
from qnc_haodou_center_${curdate}.userfollow
where to_date(createtime) between '${preday30_date}' and '${statis_date}'
group by userid;
