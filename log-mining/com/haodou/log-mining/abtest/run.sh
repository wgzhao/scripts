tday=`date +%Y-%m-%d`
mkdir backup.AB.$tday
python readConf.py ABOption > ABOption.txt
sh hadoopRequest.sh
sh hadoopResponse.sh
sh hadoopABUser.sh
hdfs dfs -cat /user/zhangzhonghui/logcount/abUser/* | python selectPolicy.py > abUser.txt
sh methodInfoAB.sh
#sh followAB.sh
sh resideAB.sh

