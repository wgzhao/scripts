
--菜谱成果照发布
--本处指标按日累计
insert overwrite table bing.rpt_rcp_photopublish_wm partition (statis_week='${statisweek_firstday}')
select
sum(photo_num) as photo_num,      
sum(usr_num) as usr_num,      
sum(web_photonum) as web_photonum,    
sum(android_photonum) as android_photonum,
sum(iphone_photonum) as iphone_photonum,
sum(web_usrnum) as web_usrnum,      
sum(android_usrnum) as android_usrnum,  
sum(iphone_usrnum) as iphone_usrnum   
from (
select
to_date(createtime) as statis_date,
sum(imagenum) as photo_num,
count(distinct userid) as usr_num,
sum(case when `From`=0 then imagenum else 0 end) as web_photonum,
sum(case when `From`=1 then imagenum else 0 end) as android_photonum,
sum(case when `From`=2 then imagenum else 0 end) as iphone_photonum,
count(distinct case when `From`=0 then userid end) as web_usrnum,
count(distinct case when `From`=1 then userid end) as android_usrnum,
count(distinct case when `From`=2 then userid end) as iphone_usrnum
from haodou_photo_${curdate}.photo p 
where p.createtime between '${statisweek_firstday} 00:00:00' and '${statisweek_lastday} 23:59:59'
and p.status in (1,2)
group by to_date(createtime)
) t
;
