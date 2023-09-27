#!/bin/bash
#location: node28:/root/bin/
#####################################################################
# 通用数据库备份脚本，主要用来备份k8s平台的数据库，目前仅支持MySQL/PostgreSQL
# 需要备份的数据库用文件进行指定，文件每行指定一个数据库信息，格式如下：
# namespace|service|dbtype|username|password|[db1 db2]|mobile
# namespace k8s 上的 namespace
# service 数据库对应的 service 名称
# dbtype 指数据库类型，MySQL/mariadb均为mysql,PostgreSQL为postgresql
# username 数据库用户名
# password 数据库密码
# [db1 db2] 数据库名称，多个数据库用空格分隔，如果没有指定，则备份整个数据库
# mobile 手机号码，用于接收备份结果通知,多个手机号码用半角逗号分隔
# 如果一行用#开头，则表示该行为注释，不会执行
#
# 使用方法：
# $0 <backup_file> [entry] [entry] ...
#
# backup_file 的一个例子
#staff-center|mysql|mysql|root|Lczq@2022|umptask|15974103570
#####################################################################
. /etc/profiled.d/sms.sh
# base variables define
BASEDIR=/opt/backup
EXPIRE_DAYS=7
SEP="|"
host=$(hostname -s)
NAMESPACE=""
SERVICE=""
IP=""
PORT=""
USERNAME=""
PASS=""
DBNAMES=""
MOBILE=""

export LANG=en_US.UTF-8
export LANGUAGE=en_US.UTF-8

curdate=$(date +"%F-%H")
os=$(uname -s)
exit_code=0

if [ "$os" = "Darwin" ];then
    BASEDIR=/tmp/backup
fi

if [ $# -lt 1 ];then
    echo "Usage: $0 <filename>"
    exit 1
fi

function print_msg {
    msg=$1
    curtime=$(date +"%F %T")
    echo "[$curtime] $msg"
}

function send_sms()
{
    if [ -z "$MOBILE" ];then
        return 0
    fi
    msg=$1
    curl -H "Content-Type: application/json"  -X POST \
        -d "{\"mobiles\":\"$MOBILE\", \"content\":\"$1 (from $host)\"}" \
        'http://10.90.23.32:9090/sms/api/v1/smsSender'
}

# get  database host name and port
function get_db_info()
{
    [ -n "$NAMESPACE" ] || exit 2
    [ -n "$SERVICE" ] || exit 3
    IP=$(kubectl get svc -n $NAMESPACE $SERVICE -o jsonpath='{.spec.clusterIP}')
    PORT=$(kubectl get svc -n $NAMESPACE $SERVICE -o jsonpath='{.spec.ports[0].port}')
}

function pg_backup 
{
    print_msg "begin backup ${IP}:${PORT}"
    if [ "$os" = "Darwin" ];then
        print_msg "local env, skip it"
        return 
    fi
    cmd="PGPASSWORD='${PASS}' pg_dump -Fp -Z 5 -h ${IP} -p ${PORT} -U ${USERNAME} "
    if [ -n "$DBNAMES" ];then
        cmd="$cmd $DBNAMES"
    fi

    curdir="${BASEDIR}/${NAMESPACE}/${SERVICE}"
    [ -d ${curdir} ] || mkdir -p $curdir
    cmd="${cmd} -f ${curdir}/${curdate}.sql.gz"
    eval $cmd 
    # get file size
    size=$(ls -sh ${curdir}/${curdate}.sql.gz |awk '{print $1}')
    if [ $? -ne 0 ];then
        print_msg "backup failed"
        send_sms "备份 $NAMESPACE:$SERVICE(大小: $size) 数据库失败"
        send_wechat "【失败】备份 $NAMESPACE:$SERVICE(大小: $size) 数据库失败"
       (( exit_code = exit_code + 1 ))
    else
        print_msg "backup success"
        send_sms "备份 $NAMESPACE:$SERVICE(大小: $size) 数据库成功"
        send_wechat "【成功】备份 $NAMESPACE:$SERVICE(大小: $size) 数据库成功"
    fi
}

function mysql_backup 
{
    # mysql server is alive or not ?
    print_msg "begin process $IP:${PORT}"
    if [ "$os" = "Darwin" ];then
        print_msg "local env, skip it"
        return 
    fi
    print_msg "checkout mysql server is alive or not"
    ret=$(MYSQL_PWD="${PASS}" mysqladmin -h${IP} -P${PORT} -u${USERNAME} ping )
    if [ "x$ret" != "xmysqld is alive" ];then
        print_msg "mysql server ${2}:${3} gone away, exit code: $ret"
        (( exit_code = exit_code + 1 ))
        return $exit_code
    fi

    print_msg "begin backup...."
    cmd="mysqldump --no-tablespaces --default-character-set=utf8 --single-transaction --quick -h${IP} -P${PORT} -u${USERNAME} -p'${PASS}' "
    #check enable binlog or not
    logbin=$(mysql -h${IP} -P${PORT} -u${USERNAME} -p"${PASS}" -N --batch -e "select @@log_bin")
    if [ "$logbin" = "1" ] ;then
        cmd="${cmd} --master-data=1 " 
    fi
    if [ -n "$DBNAMES" ];then
        cmd="${cmd} --databases ${DBNAMES}"
    else
        cmd="${cmd} --all-databases "
    fi
    curdir="${BASEDIR}/${NAMESPACE}/${SERVICE}"
    [ -d ${curdir} ] || mkdir -p $curdir
    set -o pipefail
    eval $cmd |gzip >${curdir}/${curdate}.sql.gz
    # get file size
    size=$(ls -sh ${curdir}/${curdate}.sql.gz |awk '{print $1}')
    if [ $? -ne 0 ];then
        (( exit_code = exit_code + 1 ))
        print_msg "backup failed"
        send_sms "备份 $NAMESPACE:$SERVICE(大小: $size) 数据库失败"
        send_wechat "【失败】备份 $NAMESPACE:$SERVICE(大小: $size) 数据库"
    fi
    ( cd ${curdir} && md5sum ${curdate}.sql.gz > ${curdate}.sql.gz.md5sum )
    set +o pipefail
}
# main
# filter comment and blank lines
while read line
do
    if [ "${line:0:1}" != "#" -a "${line:0:1}" != " " ];then
        oldifs="$IFS"
        IFS=${SEP} arr=($line)
        IFS="$oldifs"
        NAMESPACE=${arr[0]}
        SERVICE=${arr[1]}
        get_db_info
        if [ -z "$IP" ];then
            echo "current namespace: $namespace, service: $service not found"
            (( exit_code = exit_code + 1 ))
            continue
        fi
        dbtype=${arr[2]}
        USERNAME=${arr[3]}
        PASS=${arr[4]}
        DBNAMES=${arr[5]}
        MOBILE=${arr[6]}
        if [ "$dbtype" = "mysql" ];then
            mysql_backup 
        elif [ "$dbtype" = "postgresql" -o "$dbtype" = "pg" ];then
            pg_backup 
        else
            echo "unknown dbtype: $dbtype"
            (( exit_code = exit_code + 1 ))
            continue
        fi
    fi
done <"$1"

#clean old backup
print_msg "cleanup old backups"
if [ "$os" = "Darwin" ];then
    print_msg "local env, skip it"
else
    find $BASEDIR -type f -mtime +${EXPIRE_DAYS} -name *.sql.gz |xargs rm -f '{}' \;
fi
exit $exit_code
