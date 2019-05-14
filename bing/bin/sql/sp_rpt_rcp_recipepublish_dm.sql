
--菜谱发布日指标
--更新最近7天

insert overwrite table bing.rpt_rcp_recipepublish_dm
select *
from bing.rpt_rcp_recipepublish_dm
where statis_date < '${preday7_date}'
;

insert into table bing.rpt_rcp_recipepublish_dm
select
to_date(nvl(rp.reviewtime,rp.updatetime)) as statis_date,
count(rp.recipeid) as recipe_num,
count(distinct rp.userid) as user_num,
count(case when rp.PostFrom=0 then rp.recipeid end) as web_recipenum,
count(case when rp.PostFrom=1 then rp.recipeid end) as android_recipenum,
count(case when rp.PostFrom=2 then rp.recipeid end) as iphone_recipenum,
count(distinct case when rp.PostFrom=0 then rp.userid end) as web_usernum,
count(distinct case when rp.PostFrom=1 then rp.userid end) as android_usernum,
count(distinct case when rp.PostFrom=2 then rp.userid end) as iphone_usernum,
count(distinct case when rp.status=0 then rp.recipeid end) as recipe_passnum,
count(distinct case when rp.status=0 then rp.userid end) as user_passnum,
count(case when rp.PostFrom=0 and rp.status=0 then rp.recipeid end) as web_passnum,
count(case when rp.PostFrom=1 and rp.status=0 then rp.recipeid end) as android_passnum,
count(case when rp.PostFrom=2 and rp.status=0 then rp.recipeid end) as iphone_passnum,
count(distinct case when rp.PostFrom=0 and rp.status=0 then rp.userid end) as web_passusr,
count(distinct case when rp.PostFrom=1 and rp.status=0 then rp.userid end) as android_passusr,
count(distinct case when rp.PostFrom=2 and rp.status=0 then rp.userid end) as iphone_passusr,
count(case when rate>=3 then rp.recipeid end) as star_num,
count(case when record=1 then rp.recipeid end) as index_num
from haodou_recipe_${curdate}.Recipe rp
where nvl(rp.reviewtime,rp.updatetime) between '${preday7_date} 00:00:00' and '${statis_date} 23:59:59'
group by to_date(nvl(rp.reviewtime,rp.updatetime))
;

insert overwrite table bing.rpt_rcp_recipepublish_sum
select
count(rp.recipeid) as recipe_num,
count(distinct rp.userid) as user_num,
count(case when PostFrom=0 then rp.recipeid end) as web_recipenum,
count(case when PostFrom=1 then rp.recipeid end) as android_recipenum,
count(case when PostFrom=2 then rp.recipeid end) as iphone_recipenum,
count(distinct case when PostFrom=0 then rp.userid end) as web_usernum,
count(distinct case when PostFrom=1 then rp.userid end) as android_usernum,
count(distinct case when PostFrom=2 then rp.userid end) as iphone_usernum,
count(case when rate=1 then rp.recipeid end) as star1_num,
count(case when rate=2 then rp.recipeid end) as star2_num,
count(case when rate=3 then rp.recipeid end) as star3_num,
count(case when rate=4 then rp.recipeid end) as star4_num,
count(case when rate=5 then rp.recipeid end) as star5_num,
count(case when record=1 then rp.recipeid end) as index_num
from haodou_recipe_${curdate}.Recipe rp
where rp.status=0
;
