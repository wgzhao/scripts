
--广场试用申请名单
--mysql -h10.1.1.8 -ubi -pbi_haodou -P33060

mysql -h10.1.1.8 -ubi -pbi_haodou -P33060 -e "
set names utf8;
set @xid = 771;

drop table if exists test.trialapplyhistory;
create table if not exists test.trialapplyhistory as
select s0.userid, 
max(s0.shoporderid) as lastshoporderid,
count(distinct s0.shopgoodsid) as apply_cnt, 
count(distinct case when s0.status=1 then s0.shopgoodsid end) as success_cnt
from haodou_shop.ShopOrder s0,
(select distinct userid from haodou_shop.ShopOrder where shopgoodscate=3 and shopgoodsid=@xid) u
where s0.shopgoodscate=3 and s0.shopgoodsid<@xid
and s0.userid=u.userid
group by s0.userid
;

select
dd.shoporderid, dd.userid, u.username, dd.buytime, dd.realname, 
coalesce(cd.provincename,concat('cityid:',dd.cityid)) as provincename, 
coalesce(cd.cityname,concat('cityid:',dd.cityid)) as cityname,
coalesce(ad.areaname,concat('areaid:',dd.areaid)) as areaname, 
dd.address, dd.postcode, dd.phone, dd.mobilephone, dd.remarks,
concat(coalesce(history.lastbuytime,''),' ',coalesce(history.lastshopgoodsname,'')) as last_apply,
coalesce(history.apply_cnt,0) as apply_cnt, coalesce(history.success_cnt,0) as success_cnt,
case when dd.address='' then 'app' else 'web' end as apply_way
from
(select shoporderid, userid, buytime, realname, city as cityid, area as areaid, address, postcode, phone, mobilephone, remarks
from haodou_shop.ShopOrder
where shopgoodscate=3 and shopgoodsid=@xid
) dd 
left join haodou_passport.User u on (dd.userid=u.userid)
left join haodou.CityProvince cd on (dd.cityid=cd.cityid)
left join haodou.Area ad on (dd.areaid=ad.areaid)
left join (
select s.userid, s.buytime as lastbuytime, s.shopgoodsname as lastshopgoodsname,
h0.apply_cnt, h0.success_cnt
from haodou_shop.ShopOrder s,
test.trialapplyhistory h0 
where s.shoporderid=h0.lastshoporderid
) history on (dd.userid=history.userid)
;
" > trialapply.csv
