
insert overwrite table bing.rpt_app_regdur_dm partition (statis_date='${statis_date}')
select tt.app_id, '*' as version_id,
count(1) as total_cnt,
count(case when uu.userid is not null then 1 end) as success_cnt,
coalesce(avg(case when uu.userid is not null then tt.request_dur end),0) as avg_dur,
count(case when tt.regway='email' and uu.userid is not null then 1 end) as email_cnt,
coalesce(avg(case when tt.regway='email' and uu.userid is not null then tt.request_dur end),0) as email_dur,
count(case when tt.regway='phone' and uu.userid is not null then 1 end) as phone_cnt,
coalesce(avg(case when tt.regway='phone' and uu.userid is not null then tt.request_dur end),0) as phone_dur,
count(case when tt.regway='3p' and uu.userid is not null then 1 end) as 3p_cnt,
coalesce(avg(case when tt.regway='3p' and uu.userid is not null then tt.request_dur end),0) as 3p_dur
from
(select app_id, version_id, device_id, case when userid!='' then userid end as userid, begin_time, end_time, request_dur, request_info,
case 
when instr(request_info,'passport.loginbyconnect')>0 then '3p'
when instr(request_info,'passport.bindconnectstatus')>0 and instr(request_info,'common.sendcode')>0 then '3p'
when instr(request_info,'common.sendcode')>0 then 'phone'
when instr(request_info,'passport.reg')>0 then 'email'
else 'other' end as regway
from bing.dw_app_device_reglogin_dm
where statis_date='${statis_date}'
) tt left outer join
(select userid, email, mobile, regtime, regip
from haodou_passport_${curdate}.`User`
where regtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
) uu on (tt.userid is not null and tt.userid=uu.userid)
where (uu.userid is not null and uu.regtime between tt.begin_time and tt.end_time)
or (tt.userid is null)
group by tt.app_id
;

insert into table bing.rpt_app_regdur_dm partition (statis_date='${statis_date}')
select tt.app_id, tt.version_id,
count(1) as total_cnt,
count(case when uu.userid is not null then 1 end) as success_cnt,
coalesce(avg(case when uu.userid is not null then tt.request_dur end),0) as avg_dur,
count(case when tt.regway='email' and uu.userid is not null then 1 end) as email_cnt,
coalesce(avg(case when tt.regway='email' and uu.userid is not null then tt.request_dur end),0) as email_dur,
count(case when tt.regway='phone' and uu.userid is not null then 1 end) as phone_cnt,
coalesce(avg(case when tt.regway='phone' and uu.userid is not null then tt.request_dur end),0) as phone_dur,
count(case when tt.regway='3p' and uu.userid is not null then 1 end) as 3p_cnt,
coalesce(avg(case when tt.regway='3p' and uu.userid is not null then tt.request_dur end),0) as 3p_dur
from
(select app_id, version_id, device_id, case when userid!='' then userid end as userid, begin_time, end_time, request_dur, request_info,
case 
when instr(request_info,'passport.loginbyconnect')>0 then '3p'
when instr(request_info,'passport.bindconnectstatus')>0 and instr(request_info,'common.sendcode')>0 then '3p'
when instr(request_info,'common.sendcode')>0 then 'phone'
when instr(request_info,'passport.reg')>0 then 'email'
else 'other' end as regway
from bing.dw_app_device_reglogin_dm
where statis_date='${statis_date}'
) tt left outer join
(select userid, email, mobile, regtime, regip
from haodou_passport_${curdate}.`User`
where regtime between '${statis_date} 00:00:00' and '${statis_date} 23:59:59'
) uu on (tt.userid is not null and tt.userid=uu.userid)
where (uu.userid is not null and uu.regtime between tt.begin_time and tt.end_time)
or (tt.userid is null)
group by tt.app_id, tt.version_id
;
