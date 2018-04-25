insert overwrite table bing.rpt_qnc_shoprecommenddist_mm partition(ptdate='${statisdate}')
select shopid, count(1) count
from qnc_haodou_pai_${curdate}.shoprecommend
where to_date(createtime) between '${preday30_date}' and '${statis_date}'
group by shopid;
