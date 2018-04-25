tday=`date +%Y-%m-%d`
mkdir ~/data/backup.AB
#python readConf.py ABOption > ABOption.txt
#sh hadoopRequest.sh
#sh hadoopResponse.sh
#sh hadoopABUser.sh
#hdfs dfs -cat /user/zhangzhonghui/logcount/abUser/* > abUser.txt
#awk '{OFS="\t";print $2,$1}' ../log/pushChild/merge.user.r96 > abUser.txt
awk '{print $1}' abUser.txt | sort -u > ABOption.txt
sh methodInfoAB.sh
#sh followAB.sh
sh resideAB.sh


