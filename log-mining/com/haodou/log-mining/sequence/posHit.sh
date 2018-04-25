source ../util/hadoop.sh

if [ -z $1 ]; then
	echo "请指定日期！"
	exit 1
fi

day=$1

input=/user/zhangzhonghui/logcount/sequence/$day
output=/user/zhangzhonghui/logcount/posHit/$day
files=posHit.py,seqHit.py,hitItemName.py,actionInfo.py,notSeq.txt,onlySeq.txt,../column.py,actionUserInfo.py,../util/DictUtil.py,getActionItem.py,../util/DBCateName.py,cateidName.txt,../util/columnUtil.py
mapper="python,posHit.py"
reducer="python,posHit.py,merge"

basic $input $output $files $mapper $reducer

#mkdir ~/data/posHit
#hdfs dfs -text $output/p* > ~/data/posHit/$day


