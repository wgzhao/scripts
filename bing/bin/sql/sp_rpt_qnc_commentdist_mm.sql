insert overwrite table bing.rpt_qnc_commentdist_mm partition(ptdate='${statisdate}')
select userid, count(1) count
from qnc_haodou_comment_${curdate}.comment
where status = 1
and to_date(createtime) between '${preday30_date}' and '${statis_date}'
group by userid;
