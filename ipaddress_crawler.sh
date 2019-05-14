#!/bin/bash
#using old spark version
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/opt/app/spark/bin:/opt/app/shark/bin
queue=${1:-"alpha"}
user=$(whoami)
id=$(yarn application -list 2>/dev/null |grep 'ipaddresscrawler' |awk '{print $1}')
if [ -n "$id" ];then
    yarn application -kill $id
    hdfs dfs -rm -r -f -skipTrash /app-logs/hdfs/logs/$id
    hdfs dfs -rm -r -f -skipTrash /user/${user}/.sparkStaging/$id
fi

export SPARK_JAR=/opt/app/spark/assembly/target/spark-assembly_2.10-0.9.0-incubating-sources.jar

nohup /opt/app/spark/bin/spark-submiclass org.apache.spark.deploy.yarn.Client --files      file:///etc/hive/conf/hive-site.xml,$(hdfs dfs -ls  hdfs://hdcluster/bi/warehouse/mobile/lib/\*.jar |awk '{print $NF}' |grep '^hdfs' |tr '\n' ',') --jar hdfs://hdcluster/bi/warehouse/mobile/lib/iaw-1.0.jar  --class com.haodou.bi.iaw.HaodouIPAddressParser --args yarn-standalone --num-workers 3 --master-memory 1g --worker-memory 3g --worker-cores 2 --queue ${queue} --name ipaddresscrawler 2>&1 &

exit $?