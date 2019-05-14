
--广场试用申请名单

insert overwrite table bing.rpt_grp_trialapply_info partition (shopgoodsid='${xid}')
select /*+ mapjoin(dd,cd,ad,last,history)*/
dd.shoporderid, dd.userid, u.username, dd.buytime, dd.realname, 
coalesce(cd.provincename,concat('cityid:',dd.cityid)), 
coalesce(cd.cityname,concat('cityid:',dd.cityid)),
coalesce(ad.areaname,concat('areaid:',dd.areaid)), 
dd.address, dd.postcode, dd.phone, dd.mobilephone, dd.remarks,
concat(coalesce(to_date(last.buytime),''),' ',coalesce(last.shopgoodsname,'')) as last_apply,
coalesce(history.apply_cnt,0), coalesce(history.success_cnt,0),
case when dd.address='' then 'app' else 'web' end as apply_way
from
(select shoporderid, userid, buytime, realname, city as cityid, area as areaid, address, postcode, phone, mobilephone, remarks
from haodou_shop_${curdate}.ShopOrder
where shopgoodscate=3 and shopgoodsid=${xid}
) dd inner join haodou_passport_${curdate}.`User` u on (dd.userid=u.userid)
left outer join haodou_${curdate}.CityProvince cd on (dd.cityid=cd.cityid)
left outer join haodou_${curdate}.Area ad on (dd.areaid=ad.areaid)
left outer join (select t.userid, t.buytime, t.shopgoodsname
from (
select dd.shoporderid, dd.userid, dd.buytime, dd.shopgoodsid, dd.shopgoodsname,
row_number() over(partition by dd.userid order by dd.shoporderid desc) as sn
from
(select shoporderid, userid, buytime, shopgoodsid, shopgoodsname
from haodou_shop_${curdate}.ShopOrder
where shopgoodscate=3 and shopgoodsid<${xid}
) dd 
left semi join (select userid from haodou_shop_${curdate}.ShopOrder where shopgoodscate=3 and shopgoodsid=${xid}) u on (dd.userid=u.userid)
) t
where t.sn=1
) last on (dd.userid=last.userid)
left outer join (select userid, count(distinct shopgoodsid) as apply_cnt, 
count(distinct case when status=1 then shopgoodsid end) as success_cnt
from haodou_shop_${curdate}.ShopOrder
where shopgoodscate=3 and shopgoodsid<${xid}
group by userid
) history on (dd.userid=history.userid)
;
