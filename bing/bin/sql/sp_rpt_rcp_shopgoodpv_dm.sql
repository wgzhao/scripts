insert overwrite table bing.rpt_rcp_shopgoodpv_dm partition(ptdate='${statis_date}')
select t.platform ,t.shopgoodsid, t.cate, t.cnt
from (
select '0' platform, goods.shopgoodsid, goods.cate, l.cnt
from haodou_shop_${curdate}.shopgoods goods
join
(select regexp_extract(path, '^/view.php\\?id=(\\d+).*$', 1) goodsid, count(1) cnt
from logs.shop_haodou_com
where logdate='${statis_date}' and path like '/view.php%'
group by regexp_extract(path, '^/view.php\\?id=(\\d+).*$', 1)) l
on l.goodsid = goods.shopgoodsid
union all
select '1' platform, goods.shopgoodsid, goods.cate, l.cnt
from haodou_shop_${curdate}.shopgoods goods
join
(select regexp_extract(path, '^/native/shop/index.php\\?do=view&goods_id=(\\d+).*$', 1) 
goodsid, count(1) cnt
from logs.m_haodou_com
where logdate='${statis_date}' and path like '/native/shop/index.php?do=view%'
group by regexp_extract(path, '^/native/shop/index.php\\?do=view&goods_id=(\\d+).*$', 1)) l
on l.goodsid = goods.shopgoodsid) t;
