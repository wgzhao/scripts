sh queryTag.sh
sh pushview.sh
sh userMessageMatch.sh
sh userMessageCount.sh
hdfs dfs -cat /user/zhangzhonghui/logcount/userMessageCount/* > umc.all
awk -F"	" '{if($3 > 500) print $0}' umc.all > umc5
grep VOID_TAG umc.all >> umc5

