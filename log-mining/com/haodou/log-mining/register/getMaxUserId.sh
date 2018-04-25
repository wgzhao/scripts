
if [ -z $1 ]; then
	echo "未指定取得最大用户id的日期的后一天的日期"
	exit 1
fi
yd=$(python ../util/TimeUtil.py addDay $1 -1)
yd=$(python ../util/TimeUtil.py format $yd "%Y-%m-%d" "%Y%m%d")
echo $yd

hive -e "select max(userid) from hd_recipe_user_"$yd".user" > maxUid.txt

