
--好豆菜谱渠道IP集中情况
--此处目前限定为菜谱版本为5以上（用来减少统计数据条数，版本可随发布情况修改）
insert overwrite table bing.rpt_app_ipmass_ds
select /*+mapjoin (t0,t1,t)*/
t.app_id,
t.channel_id,
t.ss, t.s1, t.s2, t.s3, t.s4, t.s5, t.s6, t.s7, t.s8, t.s9, t.s10, t.s11, t.smore,
round(100.0*s1/ss,2) as p1,
round(100.0*s2/ss,2) as p2,
round(100.0*s3/ss,2) as p3,
round(100.0*s4/ss,2) as p4,
round(100.0*s5/ss,2) as p5,
round(100.0*s6/ss,2) as p6,
round(100.0*s7/ss,2) as p7,
round(100.0*s8/ss,2) as p8,
round(100.0*s9/ss,2) as p9,
round(100.0*s10/ss,2) as p10,
round(100.0*s11/ss,2) as p11,
round(100.0*smore/ss,2) as pmore
from (select app_id, channel_id, sum(devsum) as ss, 
sum(case when perdevnum=1 then devsum else 0 end) as s1,
sum(case when perdevnum<=2 then devsum else 0 end) as s2,
sum(case when perdevnum<=3 then devsum else 0 end) as s3,
sum(case when perdevnum<=4 then devsum else 0 end) as s4,
sum(case when perdevnum<=5 then devsum else 0 end) as s5,
sum(case when perdevnum<=6 then devsum else 0 end) as s6,
sum(case when perdevnum<=7 then devsum else 0 end) as s7,
sum(case when perdevnum<=8 then devsum else 0 end) as s8,
sum(case when perdevnum<=9 then devsum else 0 end) as s9,
sum(case when perdevnum<=10 then devsum else 0 end) as s10,
sum(case when perdevnum<=11 then devsum else 0 end) as s11,
sum(case when perdevnum>=12 then devsum else 0 end) as smore
from 
(select
app_id,
first_channel as channel_id,
devnum as perdevnum,
sum(devnum) as devsum
from
(select app_id, first_channel, first_userip, count(device_id) as devnum
from bing.dw_app_device_ds
where (app_id='2' and first_version>'5') or (app_id='4' and first_version>'v5')
group by app_id, first_channel, first_userip
) t0
group by app_id, first_channel, devnum
) t1
group by app_id, channel_id
) t
;
