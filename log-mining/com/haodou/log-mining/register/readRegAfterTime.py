#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../util")
import TimeUtil
import DictUtil

'''
分析一个用户使用app多久后会注册
'''
def readUserAfteTime(file):
	us={}
	uu={}
	for line in open(file):
		cols=line.split()
		if len(cols) < 4:continue
		uuid=cols[0]
		u=cols[1]
		day=cols[2]
		if u == "NULL":
			continue
		if u in us:
			if day < us[u][1]:
				#print file,u,us[u],day
				us[u]=(uuid,day)
				uu[uuid]=day
		else:
			us[u]=(uuid,day)
			uu[uuid]=day
	for line in open(file):
		cols=line.strip().split()
		if len(cols) < 4:continue
		uuid=cols[0]
		day=cols[2]
		if uuid in uu:
			if day < uu[uuid]:
				uu[uuid]=day
	for u in us:
		(uuid,day)=us[u]
		print u,uu[uuid]

def ds(uuidFile,today):
	regUserFile="/home/zhangzhonghui/data/reg/regUser."+today
	uuids={}
	for line in open(regUserFile):
		cols=line.strip().split()
		uuids[cols[0]]=[cols[1],""]
	for line in open(uuidFile):
		cols=line.split()
		if len(cols) < 4:continue
		uuid=cols[0]
		if uuid not in uuids:continue
		day=cols[2]
		if uuids[uuid][1] == "":
			uuids[uuid][1]=day
		else:
			if uuids[uuid][1] > day:
				uuids[uuid][1]=day
	ds={}
	for uuid in uuids:
		OK,lastDay=uuids[uuid]
		if OK not in ds:
			ds[OK]={}
		div=TimeUtil.daysDiv(lastDay,today)
		if div not in ds[OK]:
			ds[OK][div]=1
		else:
			ds[OK][div]+=1
	for OK in ds:
		print OK,DictUtil.sum(ds[OK])
		print ds[OK]

if __name__=="__main__":
	#readUserAfteTime(sys.argv[1])
	ds("uuid.afterTime",sys.argv[1])

