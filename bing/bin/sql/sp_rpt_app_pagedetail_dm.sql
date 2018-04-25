
insert overwrite table bing.rpt_app_pagedetail_dm partition (statis_date='${statis_date}')
select /*+ mapjoin(t,p)*/
t.app_id, t.version_id, t.page_code, coalesce(p.page_name,t.page_code), t.call_cnt, t.dev_num
from
(select app_id, version_id, page as page_code, 
count(1) as call_cnt,
count(distinct dev_uuid) as dev_num
from bing.ods_app_accesslog_dm
where statis_date='${statis_date}'
group by app_id, version_id, page
) t left outer join 
bing.dw_app_page p 
on (t.app_id=p.app_id and t.page_code=p.page_code)
;
