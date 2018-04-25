
--工单HAODOU-9301 除“菜谱专辑达人”“生活盟主”“作品达人””豆友会掌柜”外所有标签即为菜谱达人

insert overwrite table bing.ods_rcp_expert
select 
coalesce(old.userid, new.userid) as userid,
coalesce(new.username, old.username) as username,
coalesce(new.expert_type, old.expert_type) as expert_type,
coalesce(new.expert_level, old.expert_level) as expert_level,
coalesce(new.expert_style, old.expert_style) as expert_style,
coalesce(old.eff_date, new.eff_date) as eff_date,
coalesce(new.exp_date, old.exp_date) as exp_date,
coalesce(new.status, old.status) as status
from 
(select userid, username, expert_type, expert_level, expert_style, eff_date, case when status=0 then exp_date else '${statis_date}' end as exp_date, 0 as status
from bing.ods_rcp_expert
) old
full outer join
(select /*+ mapjoin(v,tg)*/ 
v.userid, v.username, '个人' as expert_type, '菜谱达人' as expert_level, '' as expert_style, '${statis_date}' as eff_date, '2020-01-01' as exp_date, 1 as status
from haodou_center_${curdate}.VipUser v 
left semi join
(select userid, concat_ws(',',collect_set(cast(tagid as string))) as expert_style 
from haodou_center_${curdate}.VipUserTag 
where tagid not in (10038,10043,10056,10060) and tagid <10070
group by userid) tg
on (v.userid=tg.userid)
where v.viptype='1' and v.status='1'
) new
on (old.userid=new.userid)
;
