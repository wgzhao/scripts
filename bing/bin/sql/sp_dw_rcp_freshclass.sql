
--菜谱新手课堂菜谱

insert overwrite table bing.dw_rcp_freshclass
select /*+ mapjoin(d1,d2)*/
d1.id, d1.rid as recipeid, to_date(d1.createtime) as begindate, to_date(coalesce(d2.createtime,from_unixtime(unix_timestamp()))) as enddate
from
(select id, rid, createtime, row_number() over(order by id asc) as sn
from haodou_mobile_${curdate}.Discovery
where type=2
) d1 left outer join 
(select id, rid, createtime, row_number() over(order by id asc) - 1 as sn
from haodou_mobile_${curdate}.Discovery
where type=2
) d2 on (d1.sn=d2.sn)
;
