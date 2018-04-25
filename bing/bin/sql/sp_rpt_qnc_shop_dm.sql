insert overwrite table bing.rpt_qnc_shop_dm partition(ptdate='${statisdate}')
select t1.*, t2.*, t3.*
from
(select count(1) new_shop_count,
count(distinct userid) new_shop_user_count,
sum(if(status=1,1,0)) as verified_count
from qnc_haodou_pai_${curdate}.shop
where to_date(createtime) = '${statis_date}') t1
full outer join
(select count(1) recommend_count,
count(distinct sr.userid) recommend_user_count,
count(distinct sr.shopid) recommend_shop_count,
count(distinct u.userid) recommend_vip_user_count
from qnc_haodou_pai_${curdate}.shoprecommend sr
left outer join 
(select userid from qnc_qunachi_user_${curdate}.user where string(vip) rlike '^102[23].*' ) u
on sr.userid = u.userid
where to_date(sr.createtime) = '${statis_date}') t2
full outer join
(select count(1) fav_count, count(distinct userid) user_fav_count, count(distinct itemid) shop_fav_count 
from qnc_qunachi_favorite_${curdate}.favorite 
where to_date(createtime)='${statis_date}' and itemtype = 5) t3;
