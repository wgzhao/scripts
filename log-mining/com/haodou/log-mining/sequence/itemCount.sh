
#hdfs dfs -text /backup/CDA39907/001/2015-03-23/* | python itemCount.py parse > ~/data/tt0207
source ../util/hadoop.sh

files=itemCount.py,sequence.py,readTagWap.py,tag.wap.txt,nutri.wap.txt,../abtest/column2.py,hitItemName.py,actionInfo.py,notSeq.txt,onlySeq.txt,../column.py,actionUserInfo.py,../util/DictUtil.py,getActionItem.py,../util/DBCateName.py,cateidName.txt,../util/columnUtil.py
mapper="python,itemCount.py,parse"
reducer="python,itemCount.py,count"

basic /backup/CDA39907/001/2015-03-*/* /user/zhangzhonghui/logcount/itemCount $files $mapper $reducer


