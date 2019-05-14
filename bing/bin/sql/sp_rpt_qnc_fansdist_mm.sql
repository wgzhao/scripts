insert overwrite table bing.rpt_qnc_fansdist_mm partition(ptdate='${statisdate}')
select followuserid, count(1) count
from qnc_haodou_center_${curdate}.userfollow
where to_date(createtime) between '${preday30_date}' and '${statis_date}'
group by followuserid;
