import sys

sys.path.append("./")
sys.path.append("../")

import dictInfo
import ccinfo

def output(lastU,ms,ds,divs):
	for message in ms:
		rv="push"
		if "pushview" in ms[message]:
			rv="view"
			if "push_received" in ms[message]:
				div = ms[message]["pushview"]-ms[message]["push_received"]
				if div < 0:
					ds["wrongNum"]+=1
					ds["wrongTime"]+=div
				else:
					if div not in divs:
						divs[div]=1
					else:
						divs[div]+=1
					ds["divTime"]+=div
					ds["divNum"]+=1
			else:
				ds["viewNoReceive"]+=1
		else:
			ds["not"]+=1
		
#u	tag	tc	sum
#u	received/view	message
#
def join():
	ds={}
	ds["divTime"]=0
	ds["divNum"]=0
	ds["wrongTime"]=0
	ds["wrongNum"]=0
	ds["not"]=0
	ds["viewNoReceive"]=0
	divs={}
	lastU=""
	ms={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 5:
			continue
		u=cols[0]
		if lastU == "":
			lastU = u
		if lastU != u:
			output(lastU,ms,ds,divs)
			lastU=u
			ts={}
			ms={}
		if cols[4].startswith("4"):
			continue
		method=cols[1]
		message=cols[2]
		time=int(cols[3])
		if message not in ms:
			ms[message]={}
		ms[message][method]=time
	if lastU != "":
		output(lastU,ms,ds,divs)
	wrongTime=ds["wrongTime"]
	wrongNum=ds["wrongNum"]
	divTime=ds["divTime"]
	divNum=ds["divNum"]
	print "view but not received:%d\n"%(ds["viewNoReceive"])
	print "not:%d\n"%(ds["not"])
	print "wrong:%d\t%d\t%f\n"%(wrongTime,wrongNum,float(wrongTime)/(wrongNum+1e-16))
	print "div:%d\t%d\t%f\n"%(divTime,divNum,float(divTime)/(divNum+1e-16))
	print dictInfo.info(divs)
	print ccinfo.info(divs)

if __name__=="__main__":
	join()

