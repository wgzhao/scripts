
--好豆BI周报结果表8-去哪吃美食发布

insert overwrite table bing.rpt_qnc_paishare_wm partition (statis_week='${statisweek_firstday}')
select 
coalesce(cityid,'*') as city_id,
count(shareid) as share_num,
count(distinct userid) as user_num,
count(case when `from`=0 then shareid end) as web_share_num,
count(case when `from`=1 then shareid end) as android_share_num,
count(case when `from`=2 then shareid end) as iphone_share_num,
count(distinct case when `from`=0 then userid end) as web_user_num,
count(distinct case when `from`=1 then userid end) as android_user_num,
count(distinct case when `from`=2 then userid end) as iphone_user_num
from (
select shareid, coalesce(cityid,0) as cityid, userid, `from`
from qnc_haodou_pai_${curdate}.PaiShare
where createtime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and status=1
) t
group by cityid with rollup
;
