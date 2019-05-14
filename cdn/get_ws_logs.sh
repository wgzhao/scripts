#!/bin/bash
# 网宿CDN日志下载程序
# 
# log format
# 117.136.30.89 - - [27/Sep/2014:00:04:10 +0800] "GET http://img4.hoto.cn/proxy/? HTTP/1.1" 200 34457 \
# "http://m.haodou.com/" "Mozilla/5.0 (compatible; MSIE 9.0; Windows Phone OS 7.5; Trident/5.0; IEMobile/9.0; NOKIA; Nokia 710)"

TMP_FILE="/tmp/get_wscdn_logs_${USER}_`date +"%F"`.log"

#params set
hdfsdir="/backup/cdn"
url="ftp://hoto:Hoto123456@ftp.wslog.chinanetcenter.com"

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
	dl.hoto.cn)
fi

shift $((OPTIND-1))
OS=$(uname -s)
if [ "$OS" = "Darwin" ];then
	offset=${1:-"1"}
	rundate=$(date -v-${offset}d +"%Y-%m-%d")
	#dashdate=$(date -v-${offset}d +"%Y-%m-%d")
else
	offset=${1:-"1 day ago"}
	rundate=$(date -d "$offset"  +"%Y-%m-%d")
	#dashdate=$(date -d "$rundate" +"%Y-%m-%d")
fi

for domain in ${domains[@]}
do
    #filepath like /img1.hoto.cn/2014-09-29-0000-2330_img1.hoto.cn.log.gz
    filepath="/${domain}/${rundate}-0000-2330_${domain}.cn.log.gz"
    esdomain=$(echo $domain |tr '.' '_')
    filename=ws-${esdomain}.log
    uploaddir=${hdfsdir}/${esdomain}/${rundate}
	
	if [ "$FORCE" = 0 ];then
    	hdfs dfs -test -f ${uploaddir}/${filename}.gz
    	if [ $? -eq 0 ];then
      	  echo "${uploaddir}/${filename}.gz has exists ,skip it"
      	  continue
    	fi
	fi
	    
    echo "download from ${url}${filepath}..."
    result=$(axel -a -n 3 -o ${filename}.gz  ${url}${filepath} >>$TMP_FILE   2>&1)
    # if [ $? -ne 0 ];then
#         echo $result |grep 'File not found'
#         if [ $? -ne 0 ];then
#             echo "$filepath not exists"
#         else
#             echo "failured"
#             RETCODE=1
#         fi
#         continue
#     fi
#     echo -n "test compressed file integrity..... "
#     gunzip -t ${filename}.gz
#     if [ $? -ne 0 ];then
#         echo "False"
#         RETCODE=1
#         rm -f ${filename}.gz
#         continue
#     else
#         echo " OK "
#     fi
    #convert comression method to lzo
    #gunzip ${filename}.gz
    #lzop -U ${filename}
    # create necessary directory
    hdfs dfs -mkdir -p $uploaddir
    hdfs dfs -moveFromLocal -f ${filename}.gz $uploaddir
    #rm -f *.gz
done
exit $RETCODE
