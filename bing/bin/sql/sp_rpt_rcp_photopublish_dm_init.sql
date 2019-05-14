
--成果照发布日指标（初始化）

insert overwrite table bing.rpt_rcp_photopublish_dm
select
to_date(p.createtime) as statis_date,
sum(p.imagenum) as photo_num,
count(distinct p.userid) as usr_num,
sum(case when p.`From`=0 then p.imagenum else 0 end) as web_photonum,
sum(case when p.`From`=1 then p.imagenum else 0 end) as android_photonum,
sum(case when p.`From`=2 then p.imagenum else 0 end) as iphone_photonum,
count(distinct case when p.`From`=0 then p.userid end) as web_usrnum,
count(distinct case when p.`From`=1 then p.userid end) as android_usrnum,
count(distinct case when p.`From`=2 then p.userid end) as iphone_usrnum
from haodou_photo_${curdate}.photo p 
where p.status=1
group by to_date(p.createtime)
;
