#!/bin/bash
# location: nn01:/home/hdfs/bin:hdfs
# check bigdata and relative services status

function send_sms()
{
    content="$1"
    mobiles="15974103570"
    if [ $# -eq 2 ];then
      mobiles="$2"
    fi
    host=$(hostname -s)
    curl -H "Content-Type: application/json"  -X POST \
        -d "{\"mobiles\":\"$mobiles\", \"content\":\"$content (from $host)\"}" \
        'http://infa02:5002/api/v1/smsSender'
}

# get kerberos principal
kinit -kt /etc/security/keytabs/hdfs.headless.keytab hdfs-lczq@LCZQ.COM

# check hdfs namenode status
namenode_status=$(hdfs haadmin -getAllServiceState |awk '{print $2}' |sort | tr '\n' ' ')
if [ "$namenode_status" != "active standby " ];then
    send_sms "HDFS namenode 状态异常 $namenode_status"
    exit 1
fi

# check hdfs datanode status
ret=$(hdfs dfsadmin -report -dead  | awk '/Dead datanodes/ {print $3}')
if [ "$ret" != "(0):" ];then
    send_sms "HDFS dead datanode 个数 $ret"
    exit 2
fi

# check hdfs write

ret=$(hdfs dfs -touchz /tmp/hdfs_write 2>&1)

if [ $? -ne 0 ];then
    send_sms "HDFS write 无法写入 $ret"
    exit 3
fi

# check hdfs read
ret=$(hdfs dfs -get /tmp/hdfs_write .)

if [ $? -ne 0 ];then
    send_sms "HDFS read 无法读取 $ret"
    exit 4
fi

# check hive status

kinit  -kt /etc/security/keytabs/hive.service.keytab hive/$(hostname -f)@LCZQ.COM

## query hive
num=$(hive --silent=true --showHeader=false --outputformat=csv   \
  -e "select count(*) from odsuf.allbranch where logdate='20220105'" \
  2>/dev/null | tr -d "'")

if [ ! "x$num" = "x86" ];then
    send_sms "Hive 查询异常"
    exit 5
fi

## trino 
num=$(trino --output-format=csv --server nn01:59999 --catalog hive --user hive \
  --execute "select count(*) from odsuf.allbranch where logdate='20220105'" \
  2>/dev/null | tr -d '"')

if [ ! "x$num" = "x86" ];then
    send_sms "Trino 查询异常"
fi
