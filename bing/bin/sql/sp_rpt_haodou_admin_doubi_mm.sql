
--管理员豆币发放报表
insert overwrite table bing.rpt_haodou_admin_doubi_mm partition (statis_month='${statis_month}')
select operid, opername, sum(usernum) as usercnt, sum(doubi)
from bing.dw_haodou_adminlog_doubi
where statis_month='${statis_month}'
group by operid, opername
;
