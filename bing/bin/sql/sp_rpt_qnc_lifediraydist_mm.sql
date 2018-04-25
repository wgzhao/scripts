insert overwrite table bing.rpt_qnc_lifediraydist_mm partition(ptdate='${statisdate}')
select userid, count(1) count
from qnc_haodou_pai_${curdate}.lifediary
where to_date(createtime) between '${preday30_date}' and '${statis_date}'
group by userid;
