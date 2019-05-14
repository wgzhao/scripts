source ../util/hadoop.sh

if [ -z $1 ]; then
	echo "请指定日期！"
	exit 1
fi

day=$1

input=/user/zhangzhonghui/logcount/sequence/$day
output=/user/zhangzhonghui/logcount/seqHit/
files=seqHit.py,hitItemName.py,actionInfo.py,notSeq.txt,onlySeq.txt,../column.py,actionUserInfo.py,../util/DictUtil.py,getActionItem.py,../util/DBCateName.py,cateidName.txt,../util/columnUtil.py
mapper="python,seqHit.py"

basicNoReduce $input $output $files $mapper

mkdir ~/data/seqHit
hdfs dfs -text $output/p* | python seqHit.py merge > ~/data/seqHit/$day


