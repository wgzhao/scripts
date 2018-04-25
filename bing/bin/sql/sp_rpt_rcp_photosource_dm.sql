
--$$ script_file: sp_rpt_rcp_photosource_dm.sql
--$$ script_name: 成果照发布来源
--$$ git: bing/bin/sql/sp_rpt_rcp_photosource_dm.sql
--$$ command: ${wd}/sqlexec.py --sql=${wd}/sql/sp_rpt_rcp_photosource_dm.sql --date=$yyyymmdd
--$$ source: hive#haodou_photo_${curdate}.photo / haodou_mobile_${curdate}.ShowTopic / haodou_mobile_${curdate}.Discovery
--$$ result: hive#bing.rpt_rcp_photosource_dm

--更新菜谱新手课堂菜谱
insert overwrite table bing.dw_rcp_freshclass
select /*+ mapjoin(d1,d2)*/
d1.id, d1.rid as recipeid, to_date(d1.createtime) as begindate, to_date(coalesce(d2.createtime,from_unixtime(unix_timestamp()))) as enddate
from
(select id, rid, createtime, row_number() over(order by id asc) as sn
from haodou_mobile_${curdate}.Discovery
where type=2
) d1 left outer join 
(select id, rid, createtime, row_number() over(order by id asc) - 1 as sn
from haodou_mobile_${curdate}.Discovery
where type=2
) d2 on (d1.sn=d2.sn)
;

--更新最近7天
insert overwrite table bing.rpt_rcp_photosource_dm
select *
from bing.rpt_rcp_photosource_dm
where statis_date < '${preday7_date}'
;

insert into table bing.rpt_rcp_photosource_dm
select /*+ mapjoin(p,fc,st)*/
to_date(p.createtime) as statis_date,
case
when p.recipeid!=0 then '菜谱成果照'
when p.topicid!=0 then '主题成果照'
else '晒一晒成果照'
end as source_type,
case
when p.recipeid!=0 and p.topicid!=0 then '主题菜谱'
when p.recipeid!=0 and fc.recipeid is not null then '新手课堂菜谱'
when p.recipeid!=0 and fc.recipeid is null then '标准菜谱'
when p.topicid!=0 and st.topicname is not null then st.topicname
when p.topicid!=0 and st.topicname is null then concat('未知主题',p.topicid)
else '晒一晒'
end as source_name,
sum(p.imagenum) as photo_num,
count(distinct p.userid) as usr_num,
sum(case when p.position!='' then p.imagenum else 0 end) as loc_photo_num
from
(select id as photoid, coalesce(rid,0) as recipeid, 
coalesce(topicid,0) as topicid, userid, position, createtime, imagenum
from haodou_photo_${curdate}.Photo
where createtime between '${preday7_date} 00:00:00' and '${statis_date} 23:59:59'
and status=1) p 
left outer join 
(select recipeid
from bing.dw_rcp_freshclass 
where begindate>='${preday7_date}' and enddate<='${statis_date}') fc on (p.recipeid=fc.recipeid)
left outer join
(select id as topicid, title as topicname
from haodou_mobile_${curdate}.ShowTopic) st on (p.topicid=st.topicid)
group by to_date(p.createtime),
case
when p.recipeid!=0 then '菜谱成果照'
when p.topicid!=0 then '主题成果照'
else '晒一晒成果照'
end,
case
when p.recipeid!=0 and p.topicid!=0 then '主题菜谱'
when p.recipeid!=0 and fc.recipeid is not null then '新手课堂菜谱'
when p.recipeid!=0 and fc.recipeid is null then '标准菜谱'
when p.topicid!=0 and st.topicname is not null then st.topicname
when p.topicid!=0 and st.topicname is null then concat('未知主题',p.topicid)
else '晒一晒'
end
;
