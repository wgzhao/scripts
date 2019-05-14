insert overwrite table bing.rpt_rcp_phonebind_dm partition(ptdate='${statis_date}')
select t.cnt, o.pv, o.uv
from(
select count(1) cnt
from pig.ods_m001 pig
where ptdate='${statis_date}' 
and get_json_object(json_msg, '$.msg.mobile') is not null and (get_json_object(json_msg, '$.ip') != '0.0.0.0' or get_json_object(json_msg, '$.ip') != '127.0.0.1') ) t
full outer join
(select count(1) pv, count(distinct concat(remote_addr, http_user_agent)) uv from logs.m_haodou_com where logdate='${statis_date}' and path = '/native/shop/index.php?do=bindMobile' and status=200) o;
