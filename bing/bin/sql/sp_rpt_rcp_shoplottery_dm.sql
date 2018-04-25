set hive.auto.convert.join=false;

insert overwrite table bing.rpt_rcp_shoplottery_dm partition(ptdate='${statis_date}')
select if(t.lottery_id is null, l.lotteryid, t.lottery_id) lottery_id, 
l.money money,
case when t.src='s101' then '4' when t.src='s102' then '2' else 0 end src,
l.userid
from
(select get_json_object(json_msg, '$.msg.lottery_id') lottery_id, get_json_object(json_msg, '$.source') src
from pig.ods_m001 pig
where ptdate='${statis_date}' 
and get_json_object(json_msg, '$.msg.lottery_id') is not null and (get_json_object(json_msg, '$.ip') != '0.0.0.0' or get_json_object(json_msg, '$.ip') != '127.0.0.1') ) t
right outer join
haodou_shop_${curdate}.shoplottery l
on t.lottery_id = l.lotteryid
where to_date(l.createtime) = '${statis_date}';
