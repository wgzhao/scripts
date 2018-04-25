
--成果照发布日指标

insert overwrite table bing.rpt_rcp_photocomment_dm
select
to_date(createtime) as statis_date,
count(commentid) as comment_num,
count(distinct userid) as usr_num,
count(case when platform=0 then commentid end) as web_commentnum,
count(case when platform=1 then commentid end) as android_commentnum,
count(case when platform=2 then commentid end) as iphone_commentnum,
count(distinct case when platform=0 then userid end) as web_usrnum,
count(distinct case when platform=1 then userid end) as android_usrnum,
count(distinct case when platform=2 then userid end) as iphone_usrnum
from haodou_comment_${curdate}.Comment  
where status=1 and `type`=12
group by to_date(createtime)
;
