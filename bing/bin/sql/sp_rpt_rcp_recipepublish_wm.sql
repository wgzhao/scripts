
--菜谱发布
--本处指标按日累计
insert overwrite table bing.rpt_rcp_recipepublish_wm partition (statis_week='${statisweek_firstday}')
select
sum(recipe_num) as recipe_num,      
sum(usr_num) as usr_num,      
sum(web_recipenum) as web_recipenum,    
sum(android_recipenum) as android_recipenum,
sum(iphone_recipenum) as iphone_recipenum,
sum(web_usrnum) as web_usrnum,      
sum(android_usrnum) as android_usrnum,  
sum(iphone_usrnum) as iphone_usrnum   
from (
select
to_date(nvl(rp.reviewtime,rp.updatetime)) as statis_date,
count(rp.recipeid) as recipe_num,
count(distinct rp.userid) as usr_num,
count(case when PostFrom=0 then rp.recipeid end) as web_recipenum,
count(case when PostFrom=1 then rp.recipeid end) as android_recipenum,
count(case when PostFrom=2 then rp.recipeid end) as iphone_recipenum,
count(distinct case when PostFrom=0 then rp.userid end) as web_usrnum,
count(distinct case when PostFrom=1 then rp.userid end) as android_usrnum,
count(distinct case when PostFrom=2 then rp.userid end) as iphone_usrnum
from haodou_recipe_${curdate}.recipe rp
where nvl(rp.reviewtime,rp.updatetime) between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and rp.status=0 --0通过 --排除10待审核。待审核中有大量垃圾信息
group by to_date(nvl(rp.reviewtime,rp.updatetime))
) t
;
