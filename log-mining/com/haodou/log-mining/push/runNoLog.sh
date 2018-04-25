source ../conf.sh

today=$1

mkdir /data/push_tag/$today

#cd /home/zhangzhonghui/new-log-mining/log-mining/com/haodou/log-mining/push/
ln -s ../log/mergeCateName.txt ./
ln -s ../log/partCates.txt ./
ln -s ../log/segDict.txt ./

sh idf.sh

#更新随机推送下的点击率
cat $home_local_dir/data/push/*/score.detail.txt | grep random | python updateRate.py > itemRate.random.txt 

sh match.sh $today

hdfs dfs -mkdir $root_hdfs_dir/logcount/push/user_mid/$today
hdfs dfs -text $root_hdfs_dir/logcount/push/userTags/$today/* | python userTags.py > ./users.tags.noDate
#/data/push_tag/$today/users.tags 
hdfs dfs -rm $root_hdfs_dir/logcount/push/user_mid/$today/*
hdfs dfs -put ./users.tags.noDate $root_hdfs_dir/logcount/push/user_mid/$today
cat ./users.tags.noDate | python packDate.py $today > /data/push_tag/$today/users.tags 



python package.py packItem $today > /data/push_tag/$today/message.cmd
hdfs dfs -mkdir $root_hdfs_dir/logcount/push/message/$today
hdfs dfs -put /data/push_tag/$today/message.cmd $root_hdfs_dir/logcount/push/message/$today


