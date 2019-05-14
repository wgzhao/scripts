#!/bin/bash
#获取帝联CDN的访问日志
# 若您日志生成间隔为一天(单个域名一天一个日志文件),那么可以请求接口:
# http://runreport.dnion.com/DCC/logDownLoad.do?user=&password=&domain=&date= 若为前一天的日志可请求:
# http://runreport.dnion.com/DCC/logDownLoad.do?user=&password=&domain= 若请求整月的日志,则日期格式与 201101 类似,例如以下为请求 2010 年 9 月份日志
# http://runreport.dnion.com/DCC/logDownLoad.do?user=&password=&domain=&date=201009;
# log format
#113.105.139.205 - - [27/Sep/2014:00:01:42 +0800] "GET http://img2.hoto.cn/attachment/forum/201110/27/1057384i8l64i8ij47s7ss.jpg HTTP/1.1" \
# 200 299192 "http://m.haodou.com/topic-127648.html?f=m" "Mozilla/5.0 (Linux; U; Android 2.3.4; zh-cn; MT15i Build/4.0.2.A.0.62) AppleWebKit/533.1 \
# (KHTML, like Gecko) Version/4.0 Mobile Safari/533.1 MicroMessenger/5.3.1.51_r733746.462 NetType/WIFI"

#2015-02-02
#更简答的下载方式，直接用下面的URL
#ftp://C2B7B919CB57D2250796101C3630963C:90168ACC875188A9DC6442A2E287AED801748E37858AE78C2971DA7A80EE4D35CF990A480DC6A5177112342AB83B6357@123.150.180.216:55621/00003580/img4.hoto.cn_20150201.gz
TMP_FILE="/tmp/get_dnion_logs_${USER}_`date +"%F"`.log"
hdfsdir="/backup/cdn"
user="hongtu"
passwd="qtg-Sr8-yQw-XZG"

FORCE=0

#return code
RETCODE=0

function usage() {
	echo "$0 [-d <domain>] [-f] [-h] [date]"
	echo -e "-d <domain,domain> \t download specified domain(s) log,seperated by comma"
	echo -e "-f \t force download whether has exists"
	echo -e "-h \t print uage and exit"
	exit 2
}

while getopts "d:fh" 2>/dev/null opt
do
        case $opt in
        d) domains=$(echo $OPTARG |tr ',' ' ')
        ;;
        f) FORCE=1
        ;;
        h|*) usage
        ;;
        esac
done

#remove all options
if [ "$domains" = "" ];then
	domains=(img1.hoto.cn 
	img2.hoto.cn 
	img3.hoto.cn
	img4.hoto.cn 
	avatar0.hoto.cn 
	avatar1.hoto.cn
	recipe0.hoto.cn
	recipe1.hoto.cn
	pai0.hoto.cn
	pai1.hoto.cn
	head0.hoto.cn 
	head1.hoto.cn
	dl.hoto.cn
	v.hoto.cn)
fi
shift $((OPTIND-1))
OS=$(uname -s)
if [ "$OS" = "Darwin" ];then
	offset=${1:-"1"}
	rundate=$(date -v-${offset}d +"%Y%m%d")
	dashdate=$(date -v-${offset}d +"%Y-%m-%d")
else
	offset=${1:-"1 day ago"}
	rundate=$(date -d "$offset"  +"%Y%m%d")
	dashdate=$(date -d "$rundate" +"%Y-%m-%d")
fi

for domain in ${domains[@]}
do
    escape_domain=$(echo $domain |tr '.' '_')
    filename="dnion-${escape_domain}.log"
    uploaddir=${hdfsdir}/${escape_domain}/${dashdate}
	if [ "$FORCE" = 0 ];then
        	hdfs dfs -test -f ${uploaddir}/${filename}.gz
    	if [ $? -eq 0 ];then
      	  echo "${uploaddir}/${filename}.gz has exists ,skip it"
      	  continue
    	fi
	fi
    #downloadurl=$(curl -sS -I  "http://runreport.dnion.com/DCC/logDownLoad.do?user=${user}&password=${passwd}&domain=${domain}&date=${rundate}" |awk '/Location:/ {print $2}' | sed 's/111.161.19.216/123.150.180.216/g' | tr -d '\r')
	#downloadurl="ftp://C2B7B919CB57D2250796101C3630963C:90168ACC875188A9DC6442A2E287AED801748E37858AE78C2971DA7A80EE4D35CF990A480DC6A5177112342AB83B6357@123.150.180.216:55621/00003580/${domain}_${rundate}.gz"
	downloadurl="http://runreport.dnion.com/DCC/logDownLoad.do?user=${user}&password=${passwd}&domain=${domain}&date=${rundate}&isp=CT"
    echo "download from $downloadurl"
    wget -O ${filename}.gz ${downloadurl} >>$TMP_FILE   2>&1
    # if [ $? -ne 0 ];then
#         echo "failure to download $filename"
#         rm -f ${filename}.gz
#         RETCODE=1
#         continue
#     fi
    #     echo "download successfully!"
    # echo -n "test compressed file integrity..... "
    # gunzip -t ${filename}.gz
    # if [ $? -ne 0 ];then
    #     echo "False"
    #     RETCODE=1
    #     rm -f ${filename}.gz
    #
    # else
    #     echo "Success"
		
	    #echo "convert compression format"
	    #gunzip ${filename}.gz
	    #lzop -U ${filename}
	    # create necessary directory
	    echo "put it to hdfs"
	    hdfs dfs -mkdir -p $uploaddir
	    hdfs dfs -moveFromLocal -f ${filename}.gz $uploaddir
		#rm -f *.gz
	#fi
done

exit $RETCODE
    
