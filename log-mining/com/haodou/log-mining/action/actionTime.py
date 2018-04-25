#encoding=utf-8

import sys
sys.path.append("./")

from app_action import *
from readActivityDoc import *

def output(lastPage,startTime,endTime,ps):
	div=endTime-startTime+0.1
	if lastPage not in ps:
		ps[lastPage]=[0,0,0,0]
	ps[lastPage][0]+=div
	ps[lastPage][1]+=1
	if div > 2000:
		ps[lastPage][2]+=div
		ps[lastPage][3]+=1

def countTime():
	lastUuid=""
	ups={}
	ps={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		action=Action.readAction(cols)
		if lastUuid == "":
			lastUuid=action.user.uuid
		if lastUuid != action.user.uuid:
			lastUuid=action.user.uuid
			ups={}
		page=action.page
		time=action.time
		if page in ups and (action.action == "A1003" or action.action == "A1001"):
			output(page,ups[page],time,ps)
		if action.action == "A1000" or action.action == "A1002":
			ups[page]=time

	for p in ps:
		(s,n,bs,bn)=ps[p]
		if p in api2name:
			name=api2name[p]
		else:
			name=""
		print "%s\t%s\t%.4f\t%r\t%.4f\t%.4f\t%r\t%.4f\t%.4f\t%r\t%.4f"%(p,name,s,n,s/n,bs,bn,bs/(bn+1e-32),(s-bs),(n-bn),(s-bs)/(n-bn+1e-32))

def merge():
	ps={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		p=cols[0]
		if p not in ps:
			ps[p]=[cols[1],float(cols[2]),float(cols[3]),float(cols[5]),float(cols[6])]
		else:
			ps[p][1]+=float(cols[2])
			ps[p][2]+=float(cols[3])
			ps[p][3]+=float(cols[5])
			ps[p][4]+=float(cols[6])
	for p in ps:
		(name,s,n,bs,bn)=ps[p]
		print "%s\t%s\t%.4f\t%r\t%.4f\t%.4f\t%r\t%.4f\t%.4f\t%r\t%.4f"%(p,name,s,n,s/n,bs,bn,bs/(bn+1e-32),(s-bs),(n-bn),(s-bs)/(n-bn+1e-32))

if __name__ == "__main__":
	if len(sys.argv) < 2:
		countTime()
	elif sys.argv[1] == "count":
		countTime()
	elif sys.argv[1] == "merge":
		merge()
	

