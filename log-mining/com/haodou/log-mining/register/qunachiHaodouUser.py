#encoding=utf-8

import sys
import time

sys.path.append("./")
sys.path.append("../")
import column

def day(s):
	return time.strftime("%Y-%m-%d",time.strptime(s,'%d/%b/%Y:%H:%M:%S +0800'))

def dayFromInt(s):
	return time.strftime("%Y-%m-%d",time.localtime(int(s)))

def apiQunachi(line):
	cols=line.strip().split("\01")
	if len(cols) < 5:
		return None
	ip=cols[0]
	dtime=cols[2]
	path=cols[4]
	uid="0"
	p=path.find("uid=")
	if p > 0:
		e=path.find("&",p)
		if e < 0:
			e=len(path)
		uid=path[p+len("uid="):e]
		if uid != "0" and uid != "":
			return "%s\t%s\t%s\tqunachi"%(uid,ip,day(dtime))
	return None

StopMethods={
	"common.adddevietoken":1,
	"mobiledevice.initandroiddevice":1,
	"appscreen.getimage":1,
	"ad.getallad":1,
	"update.forceupdate":1,
	"update.version":1,
	"ad.getadinmobi":1,
	"notice.getcount":1,
	"common.gettime":1,
}


def apiHaodou(line):
	cols=line.strip().split("\t")
	if len(cols) < column.APP_LOG_COLUMNS:
		return
	method=cols[column.METHOD_CID]
	if method in StopMethods:
		return
	uid=column.uid(cols)
	if uid == None or uid == "":
		return None
	intTime=cols[column.TIME_CID]
	ip=cols[column.IP_CID]
	return "%s\t%s\t%s\thaodou"%(uid,ip,dayFromInt(intTime))

def getUser():
	for line in sys.stdin:
		u=apiHaodou(line)
		if u == None:
			u=apiQunachi(line)
		if u != None:
			print u

def output(lastU,ds,days):
	for day in ds:
		if day not in days:
			days[day]=[0,0,0]
		(hasHaodou,hasQunachi)=ds[day]
		if hasHaodou:
			days[day][0]+=1
		if hasQunachi:
			days[day][1]+=1
			if hasHaodou:
				print day+"\t"+lastU
				days[day][2]+=1

def reduce():
	days={}
	days["all"]=[0,0,0]
	lastU=""
	ds={}
	ds["all"]=[False,False]
	for line in sys.stdin:
		(uid,ip,day,t)=line.strip().split("\t")
		if lastU == "":
			lastU=uid
		if lastU != uid:
			output(lastU,ds,days)
			lastU=uid
			ds={}
			ds["all"]=[False,False]
		if day not in ds:
			ds[day]=[False,False]
		if t == "haodou":
			ds[day][0]=True
			ds["all"][0]=True
		elif t == "qunachi":
			ds[day][1]=True
			ds["all"][1]=True
	if lastU != "":
		output(lastU,ds,days)
	for day in days:
		print "%s\t%d\t%d\t%d"%(day,days[day][0],days[day][1],days[day][2])

def merge():
	days={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			print line.strip()
			continue
		(day,hv,qv,hqv)=cols
		if day not in days:
			days[day]=[int(hv),int(qv),int(hqv)]
		else:
			days[day][0]+=int(hv)
			days[day][1]+=int(qv)
			days[day][2]+=int(hqv)
	for day in days:
		print "%s\t%d\t%d\t%d"%(day,days[day][0],days[day][1],days[day][2])

if __name__=="__main__":
	if sys.argv[1] == "map":
		getUser()
	elif sys.argv[1] == "reduce":
		reduce()
	elif sys.argv[1] == "merge":
		merge()

