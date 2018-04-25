set hive.auto.convert.join=false;

insert overwrite table bing.rpt_rcp_shoporder_dm partition(ptdate='${statis_date}')
select if(t.order_id is null, l.shoporderid, t.order_id) order_id, 
l.buyprice,
l.shopgoodscate,
case when t.src='s101' then '4' when t.src='s102' then '2' else 0 end src,
l.userid
from
(select get_json_object(json_msg, '$.msg.order_id') order_id, get_json_object(json_msg, '$.source') src
from pig.ods_m001 pig
where ptdate='${statis_date}' 
and get_json_object(json_msg, '$.msg.order_id') is not null and (get_json_object(json_msg, '$.ip') != '0.0.0.0' or get_json_object(json_msg, '$.ip') != '127.0.0.1') ) t
right outer join
haodou_shop_${curdate}.shoporder l
on t.order_id = l.shoporderid
where to_date(l.buytime) = '${statis_date}';
