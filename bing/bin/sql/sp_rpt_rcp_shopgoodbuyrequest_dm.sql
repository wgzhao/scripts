insert overwrite table bing.rpt_rcp_shopgoodbuyrequest_dm partition(ptdate='${statis_date}')
select
t.platform, t.shopgoodsid, goods.cate, t.cnt
from(
select '0' platform, regexp_extract(referer, 'http://shop.haodou.com/view.php\\?id=(\\d+)',1) shopgoodsid, count(1) cnt
from logs.shop_haodou_com 
where logdate='${statis_date}' and (path like '%ajax.php?do=buynow%' or path like '%/ajax.php?do=lottery%') and status=200
group by regexp_extract(referer, 'http://shop.haodou.com/view.php\\?id=(\\d+)',1)
union all
select '1' platform, regexp_extract(referer, 'http://m.haodou.com/native/shop/index.php\\?do=view&goods_id=(\\d+)',1) shopgoodsid, count(1) cnt
from logs.m_haodou_com 
where logdate='${statis_date}' and (path like '%native/shop/index.php?do=orderOk%' or path like '%native/shop/index.php?do=lottery%') and status=200
group by regexp_extract(referer, 'http://m.haodou.com/native/shop/index.php\\?do=view&goods_id=(\\d+)',1) )t
join
haodou_shop_${curdate}.shopgoods goods
on t.shopgoodsid = goods.shopgoodsid;
