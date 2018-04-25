sh filterUser.sh
sh matchFilter.sh tmp
hdfs dfs -text /user/zhangzhonghui/logcount/push/userTags/tmp/p* | python userTagsFilter.py > userTag.txt

