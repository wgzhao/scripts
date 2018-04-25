
set names='utf8';

--维护应用版本对应关系表
insert overwrite table bing.dw_app_vnvc
select app_id, vc, vn, update_date, update_cnt
from
(select app_id, vc, vn, update_date, update_cnt,
row_number() over(partition by app_id, vc order by update_cnt desc) as seq
from
(select app_id, vc, vn, update_date, update_cnt
from bing.dw_app_vnvc
union all
select
nvl(parse_url(concat('http://',`host`,`path`),'QUERY','appid'),'') as app_id,
nvl(parse_url(concat('http://',`host`,`path`),'QUERY','vc'),'') as vc,
nvl(parse_url(concat('http://',`host`,`path`),'QUERY','vn'),'') as vn,
'${statis_date}' as update_date,
count(1) as update_cnt
from logs.api_haodou_com
where logdate='${statis_date}'
and `path` like '/index.php%'
group by 
nvl(parse_url(concat('http://',`host`,`path`),'QUERY','appid'),''),
nvl(parse_url(concat('http://',`host`,`path`),'QUERY','vc'),''),
nvl(parse_url(concat('http://',`host`,`path`),'QUERY','vn'),'')
) t0
) t1
where seq=1
;
