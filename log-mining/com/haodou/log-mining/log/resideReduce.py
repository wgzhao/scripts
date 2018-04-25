import sys

sys.path.append("./")
sys.path.append("../")
from dictInfo import *

MinSessionDiv=300
SmoothTimeDiv=5


def output(uid,times,us):
	us[uid]=[0,0,0]
	times=sorted(times)
	for time in times:
		lastEnd=us[uid][1]
		lastStart=us[uid][0]
		if time-lastEnd > MinSessionDiv:
			us[uid][2] += (lastEnd-lastStart+SmoothTimeDiv)
			us[uid][1]=time
			us[uid][0]=time
		else:
			us[uid][1]=time

def cut(us):
	uts={}
	for u in us:
		uts[u]=us[u][2]+(us[u][1]-us[u][0]+SmoothTimeDiv)
	return uts

def count(f):
	lastU=""
	us={}
	times=[]
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		u=cols[0]
		time=int(cols[1])
		if lastU == "":
			lastU= u
		if lastU != u:
			output(lastU,times,us)
			lastU=u
			times=[]
		times.append(time)
	if lastU != "":
		output(lastU,times,us)
		uts=cut(us)
		print staticInfo(uts)

if __name__=="__main__":
	count(sys.stdin)

