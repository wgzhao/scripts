add jar hdfs://hdcluster/udf/haodoubihiveudf.jar;
CREATE TEMPORARY FUNCTION unquote as 'com.haodou.bi.recipe.udf.HiveURLDecoder';
insert overwrite table bing.rpt_qnc_searchwordbycity_dm partition(ptdate='${statisdate}')
select m.cid, m.keyword, sum(cnt) cnt
from(
select t.cid, t.keyword, count(1) cnt
from(
select
regexp_extract(parameter_desc, 'keyword\"\;s:[0-9]+:\"(.+?)\"', 1) keyword,
regexp_extract(parameter_desc, 'cid\"\;s:[0-9]+:\"([0-9]+?)\"', 1) cid
from logs.log_php_app_log
where logdate='${statis_date}' and (appid=1 or appid=3)
and function_id like '%search.getlist%'
and regexp_extract(parameter_desc, 'keyword\"\;s:[0-9]+:\"(.*?)\"', 1) != ''
and regexp_extract(parameter_desc, 'cid\"\;s:[0-9]+:\"([0-9]+?)\"', 1) != '') t
group by t.cid,t.keyword
union all
select c.cityid cid, unquote(regexp_extract(t.path, '^/search/.+?/(.+?)$', 1)) keyword, count(1) cnt
from logs.www_qunachi_com t
join logs.ip_address_warehouse ips
on ips.ipaddress = t.remote_addr
join bing.ods_qnc_citysite c
on c.cityname = ips.city
where t.path rlike '^/search/.*?/[a-zA-Z%0-9]+$' and t.http_user_agent not rlike '[Ss]pider' and t.http_user_agent not rlike 'bot[^a-zA-Z]' and t.logdate='${statis_date}'
group by c.cityid, unquote(regexp_extract(t.path, '^/search/.+?/(.+?)$', 1))
union all
select t.cid, t.keyword, count(1) cnt
from 
(select regexp_extract(path, 'cityid=([0-9]+?)', 1) cid, unquote(regexp_extract(path, '/Search/.*?keyword=(.*?)&', 1)) keyword 
from logs.api_qunachi_com 
where logdate='${statis_date}' and path rlike '/Search/.*?keyword=.*?&') t 
group by t.cid, t.keyword
)m
group by m.cid, m.keyword;
