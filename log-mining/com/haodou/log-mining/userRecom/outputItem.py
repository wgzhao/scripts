#encoding=utf-8
#
#将entity和user的数据分开
#

import sys

sys.path.append("./")

from userRecomConf import *

sys.path.append("../util")
import TimeUtil


#
# 分离用户模型和资源模型
#
# 
#
def split(outDir="/home/data/",isAll=False):
	if isAll:
		ewf=open(outDir+"/recomData/entityinfo","w")
		uwf=open(outDir+"/recomData/userinfo","w")
	else:
		ewf=open(outDir+"/recomData_all/entityinfo","w")
		uwf=open(outDir+"/recomData_all/userinfo","w")
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if cols[1] == EntityUser:
			ewf.write(cols[0]+"\t"+"\t".join(cols[2:])+"\n")
		else:
			uwf.write(cols[0]+"\t"+"\t".join(cols[2:])+"\n")
	ewf.close()
	uwf.close()

def getLatest(date):
	lastKey=""
	lines=[]
	isLatest=False
	latestTime=TimeUtil.toIntTime(date,"%Y-%m-%d")
	for line in sys.stdin:
		cols=line.strip().split("\t")
		key=cols[0]
		if lastKey == "":
			lastKey=key
		if lastKey != key:
			if isLatest:
				for line in lines:
					print line
			lastKey=key
			lines=[]
			isLatest=False
		k2=cols[2]
		if k2 not in StatisKeys:
			try:
				time=int(cols[4])
			except:
				sys.stderr.write(line+"\n")
			if time > latestTime:
				isLatest=True
		lines.append(line.strip())
	if lastKey != "":
		if isLatest:
			for line in lines:
				print line

if __name__=="__main__":
	if sys.argv[1] == "split":
		if len(sys.argv) >= 3 and sys.argv[2] == "all":
			split()
		else:
			split("/home/data/",True)
	elif sys.argv[1] == "latest":
		getLatest(sys.argv[2])


