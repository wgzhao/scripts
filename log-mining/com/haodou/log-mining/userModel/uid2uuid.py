#encoding=utf-8
import sys
sys.path.append("./")
import userInfo

#hdfs dfs -text /user/zhangzhonghui/userModel/userInfo/$Yesterday/* | python uid2uuid.py 

def parseLine(line):
	if not line.startswith(userInfo.Uid_Fix):
		return None,None
	cols=line.strip().split("\t")
	if cols[1] == userInfo.U_Type:
		return cols[0][len(userInfo.Uid_Fix):],cols[3]
	return None,None

def uid2uuid():
	for line in sys.stdin:
		uid,uuid=parseLine(line)
		if uid != None:
			print uid+"\t"+uuid

if __name__=="__main__":
	uid2uuid()

