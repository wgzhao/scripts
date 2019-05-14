#!/bin/bash
offset=${1:-"1 day ago"}
curdate=$(date -d "$offset" +"%F")
pigfile="/home/crontab/cdn/parse_cdn_log.pig"
domains=(img1_hoto_cn 
img2_hoto_cn 
img3_hoto_cn
img4_hoto_cn 
avatar0_hoto_cn 
avatar1_hoto_cn
recipe0_hoto_cn
recipe1_hoto_cn
pai0_hoto_cn
pai1_hoto_cn
head0_hoto_cn 
head1_hoto_cn
dl_hoto_cn)
tmpdir=$(mktemp -d XXXX)
cd $tmpdir
for d in ${domains[@]}
do
    pig -b -param domain=$d -param curdate=$curdate -f $pigfile > ${d}.log
done

#package log
tar -czf cdn_url_404_$curdate.log.tar.gz *.log

# <recipent> <subject> <emailbody file> <attchment file>
/home/crontab/send_email.py lizhiyong@haodou.com "CDN访问源站状态为404的统计数据" "下面是CDN访问源站出现404状态的列表($curdate)"  cdn_url_404_$curdate.log.tar.gz
