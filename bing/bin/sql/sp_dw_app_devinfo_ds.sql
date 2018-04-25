
set mapreduce.output.fileoutputformat.compress.codec=com.hadoop.compression.lzo.LzopCodec;
set mapreduce.output.fileoutputformat.compress=true;
set hive.exec.compress.output=true;
set names='utf8';

--设备信息总表
insert overwrite table bing.dw_app_devinfo_ds
select
nvl(ds.app_id,dm.app_id) as app_id,
nvl(ds.device_id,dm.device_id) as device_id,
case when dm.device_id is null then ds.dev_uuid      when ds.device_id is null then dm.dev_uuid      when ds.last_date<'${statis_date}'  then dm.dev_uuid      else ds.dev_uuid      end as dev_uuid     ,
case when dm.device_id is null then ds.dev_imei      when ds.device_id is null then dm.dev_imei      when ds.last_date<'${statis_date}'  then dm.dev_imei      else ds.dev_imei      end as dev_imei     ,
case when dm.device_id is null then ds.dev_mac       when ds.device_id is null then dm.dev_mac       when ds.last_date<'${statis_date}'  then dm.dev_mac       else ds.dev_mac       end as dev_mac      ,
case when dm.device_id is null then ds.dev_idfa      when ds.device_id is null then dm.dev_idfa      when ds.last_date<'${statis_date}'  then dm.dev_idfa      else ds.dev_idfa      end as dev_idfa     ,
case when dm.device_id is null then ds.dev_idfv      when ds.device_id is null then dm.dev_idfv      when ds.last_date<'${statis_date}'  then dm.dev_idfv      else ds.dev_idfv      end as dev_idfv     ,
case when dm.device_id is null then ds.dev_brand     when ds.device_id is null then dm.dev_brand     when ds.last_date<'${statis_date}'  then dm.dev_brand     else ds.dev_brand     end as dev_brand    ,
case when dm.device_id is null then ds.dev_model     when ds.device_id is null then dm.dev_model     when ds.last_date<'${statis_date}'  then dm.dev_model     else ds.dev_model     end as dev_model    ,
case when dm.device_id is null then ds.dev_os        when ds.device_id is null then dm.dev_os        when ds.last_date<'${statis_date}'  then dm.dev_os        else ds.dev_os        end as dev_os       ,
case when dm.device_id is null then ds.dev_osver     when ds.device_id is null then dm.dev_osver     when ds.last_date<'${statis_date}'  then dm.dev_osver     else ds.dev_osver     end as dev_osver    ,
case when dm.device_id is null then ds.scr_width     when ds.device_id is null then dm.scr_width     when ds.last_date<'${statis_date}'  then dm.scr_width     else ds.scr_width     end as scr_width    ,
case when dm.device_id is null then ds.scr_height    when ds.device_id is null then dm.scr_height    when ds.last_date<'${statis_date}'  then dm.scr_height    else ds.scr_height    end as scr_height   ,
case when dm.device_id is null then ds.operator_name when ds.device_id is null then dm.operator_name when ds.last_date<'${statis_date}'  then dm.operator_name else ds.operator_name end as operator_name,
case when dm.device_id is null then ds.operator_code when ds.device_id is null then dm.operator_code when ds.last_date<'${statis_date}'  then dm.operator_code else ds.operator_code end as operator_code,
case when dm.device_id is null then ds.dev_rooted    when ds.device_id is null then dm.dev_rooted    when ds.last_date<'${statis_date}'  then dm.dev_rooted    else ds.dev_rooted    end as dev_rooted   ,
case when dm.device_id is null then ds.first_date    when ds.device_id is null then '${statis_date}' when ds.first_date<'${statis_date}' then ds.first_date    else '${statis_date}' end as first_date   ,
case when dm.device_id is null then ds.last_date     when ds.device_id is null then '${statis_date}' when ds.last_date<'${statis_date}'  then '${statis_date}' else ds.last_date     end as last_date    
from
(select 
app_id, device_id, dev_uuid, dev_imei, dev_mac, dev_idfa, dev_idfv,
dev_brand, dev_model, dev_os, dev_osver, scr_width, scr_height,
operator_name, operator_code, dev_rooted, first_date, last_date 
from bing.dw_app_devinfo_ds
distribute by app_id, device_id
) ds full join
(select 
app_id, device_id, dev_uuid, dev_imei, dev_mac, dev_idfa, dev_idfv,
dev_brand, dev_model, dev_os, dev_osver, scr_width, scr_height,
operator_name, operator_code, dev_rooted
from bing.dw_app_devinfo_dm
where statis_date='${statis_date}'
distribute by app_id, device_id
) dm on (ds.app_id=dm.app_id and ds.device_id=dm.device_id)
;
