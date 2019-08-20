#!/bin/bash
# uninstall HDP cluster completely

## first stop all services, run it on ambari server node

# get cluster name
cluster=$(curl -s -u admin:admin -H "X-Requested-By: ambari" -X GET  http://localhost:8080/api/v1/clusters/ |python -c "import sys,json; print json.load(sys.stdin)['items'][0]['Clusters']['cluster_name']")

curl -i -u admin:admin -H "X-Requested-By: ambari"  -X PUT  -d '{"RequestInfo":{"context":"_PARSE_.STOP.ALL_SERVICES","operation_level":{"level":"CLUSTER","cluster_name":"${cluster}"}},"Body":{"ServiceInfo":{"state":"INSTALLED"}}}'  http://localhost:8080/api/v1/clusters/${cluster}/services

sleep 60

# stop all ambari agent , execute it on all nodes
ambari-agent stop

# stop ambari serer
ambari-server stop

## ambari uninstall python utils, execute it on all nodes
python /usr/lib/ambari-agent/lib/ambari_agent/HostCleanup.py --silent --skip=users

## remove hadoop packages on all nodes
yum remove -y "*-$(hdp-select versions |tr '.-' '_')-*"

## remove ambari packages on all nodes
yum remove -y "ambari*"

## remove hdp-secified packages on all nodes
yum remove -y hdp-select* smartsense*

## remove log folders on all nodes
rm -rf /var/log/{ambari*,falcon*,hadoop*,hive*,knox*,hst*,oozie*,solr*,zookeeper*,spark*,flume*,superset*,ranger*,sqoop*,hbase*,tez*,yarn*}

## remove ambari resource folders
rm -rf /var/lib/ambari-agent* /var/lib/ambari-server*
rm -rf /usr/lib/ambari*

## Remove Hadoop folders including HDFS data on all nodes
rm -rf /hadoop/* /hdfs/hadoop

## Remove config folder on all nodes
rm -rf  /etc/ambari*  /etc/atlas  /etc/hadoop*  /etc/hbase  /etc/hive  /etc/hive-hcatalog  /etc/hst   /etc/kafka  /etc/livy2   /etc/phoenix  /etc/ranger  /etc/smartsense-activity  /etc/spark2  /etc/sqoop  /etc/tez  /etc/tez_llap  /etc/zookeeper /etc/flume 

## Remove PIDs on all nodes
rm -rf  /var/run/ambari*  /var/run/atlas  /var/run/hadoop*  /var/run/hbase  /var/run/hive /var/run/hive2  /var/run/hive-hcatalog  /var/run/hst   /var/run/kafka  /var/run/livy2   /var/run/phoenix  /var/run/ranger  /var/run/smartsense-activity  /var/run/spark2  /var/run/sqoop  /var/run/tez  /var/run/tez_llap  /var/run/zookeeper /var/run/flume 

## Remove library folders on all nodes
rm -rf /usr/lib/ambari* /usr/lib/ams* /var/lib/ambari* /var/lib/knox /var/lib/smartsense /var/lib/store /var/lib/hadoop* /var/lib/hive* /var/lib/flume 

## Delete HST from cron aon all nodes
sed  -i '/hst-scheduled/d' /var/spool/cron/root

## Remove service users on all nodes
for u in ambari-qa ams  atlas hbase  hdfs hive  infra-solr kafka  livy logsearch  mapred ranger  spark sqoop  tez yarn zookeeper yarn-ats activity_analyzer
do
	userdel -r $u
done

## Remove serivce group on all nodes
for g in livy spark ranger hdfs hadoop zookeeper hbase yarn mapred kafka hive sqoop
do
	groupdel $g
done


