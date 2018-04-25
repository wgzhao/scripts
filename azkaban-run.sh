#!/bin/bash
#rerun qnc-statistic flow 
url="http://123.150.200.217:8081"
sid=$(curl -s  -k -X POST --data "action=login&username=wgzhao&password=mlsxmlsx" ${url} |awk -F: '/session.id/ {print  $2}' |tr -d ' "')
baset=1414828523 #2014-10-1
cdate=$(date -j -f "%s" $baset +"%Y-%m-%d")
while [ "$cdate" != "2014-10-02" ]
do
    res=$(curl -k -s --get --data "session.id=$sid" --data 'ajax=executeFlow' --data 'project=data_center_scheduler' --data 'flow=logs_qnc_statistics_flow'  --data 'disbaled=["qnc_page_view_all_devices","qnc_page_view_new_devices","qnc_useractive_dm","rpt_app_push_view_dm"]' ${url}/execute)
    (( baset = $baset + 86400 ))
    cdate=$(date -j -f "%s" $baset +"%Y-%m-%d")
    echo $cdate
    echo $res
done