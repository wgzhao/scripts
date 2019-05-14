insert overwrite table bing.rpt_qnc_lifediary_dm partition(ptdate='${statisdate}')
select t1.*,
if(isnull(t2.with_shop), 0, t2.with_shop) with_shop,
if(isnull(t2.with_content), 0, t2.with_content) with_content,
if(isnull(t2.noatuser), 0, t2.noatuser) noatuser,
if(isnull(t2.atuser_count_gt_0), 0, t2.atuser_count_gt_0) atuser_count_gt_0,
if(isnull(t2.atuser_count_eq_1), 0, t2.atuser_count_eq_1) atuser_count_eq_1,
if(isnull(t2.atuser_count_eq_2), 0, t2.atuser_count_eq_2) atuser_count_eq_2,
if(isnull(t2.atuser_count_eq_3), 0, t2.atuser_count_eq_3) atuser_count_eq_3,
if(isnull(t2.atuser_count_eq_4), 0, t2.atuser_count_eq_4) atuser_count_eq_4,
if(isnull(t2.atuser_count_eq_5), 0, t2.atuser_count_eq_5) atuser_count_eq_5 
from
(select count(1) count, count(distinct userid) user_count 
from qnc_haodou_pai_${curdate}.lifediary
where to_date(createtime) = '${statis_date}') t1
full outer join
(select sum(if(shopid!=0,1,0)) with_shop,
sum(if(content != 'null' and length(content)>0,1,0)) with_content,
sum(if(atuser = 'null', 1, 0)) noatuser,
sum(if(atuser != 'null' and length(atuser) - length(regexp_replace(atuser, ',', ''))+1> 0, 1, 0)) atuser_count_gt_0,
sum(if(atuser != 'null' and length(atuser) - length(regexp_replace(atuser, ',', ''))+1= 1, 1, 0)) atuser_count_eq_1,
sum(if(atuser != 'null' and length(atuser) - length(regexp_replace(atuser, ',', ''))+1= 2, 1, 0)) atuser_count_eq_2,
sum(if(atuser != 'null' and length(atuser) - length(regexp_replace(atuser, ',', ''))+1= 3, 1, 0)) atuser_count_eq_3,
sum(if(atuser != 'null' and length(atuser) - length(regexp_replace(atuser, ',', ''))+1= 4, 1, 0)) atuser_count_eq_4,
sum(if(atuser != 'null' and length(atuser) - length(regexp_replace(atuser, ',', ''))+1= 5, 1, 0)) atuser_count_eq_5
from qnc_haodou_pai_${curdate}.lifediary
where to_date(createtime) = '${statis_date}') t2;
