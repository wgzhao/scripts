#encoding=utf-8
import sys

sys.path.append("./")
from actionUserInfo import *
from actionInfo import *

sys.path.append("../util")
import TimeUtil

import math

def readTopMethod():
	tops={}
	for line in open("topNode.txt"):
		tops[line.strip()]=1
	return tops

tops=readTopMethod()

def readUsers():
	us={}
	for line in open("new.user.1210"):
		us[line.strip()]=1
	return us

def getStr(action,ids,prefix="\n\t"):
	method=action.method
	if action.method == "search.getlist":
		scene=column.getValue(action.para,"scene")
		if scene != "":
			method=method+"-"+scene
	s=method+"("+action.getNotVoidItem()+")"
	for id in action.sons:
		if id in ids:
			s+=prefix+"_"+getStr(ids[id],ids,prefix+"\t")
	for id in action.pull:
		if id in ids:
			s+=prefix+"_"+getStr(ids[id],ids,prefix+"\t")
	return s

def outputSeqStr(lastU,lastUser,actions):
	ids={}
	for action in actions:
		if action.method in tops:
			ids[action.id]=action
		else:
			#if action.parentId in ids:
			ids[action.id]=action
	s=""
	items=sorted(ids.items(),key=lambda e:e[0])
	for id,action in items:
		if action.parentId == -1:
			s+="\n\t"+getStr(action,ids,"\n\t\t")
	print str(lastUser)+"\t"+s

mr={}
def addUser(mc1,mc2):
	for m in mc1:
		x=0
		y=0
		x=mc1[m][1]/(mc1[m][0])+mc1[m][2]
		if x <= 0:
			x=1
		else:
			x=0
		if len(mc2) > 0:
			y=1
		if m not in mr:
			mr[m]=[0,0,0,0,0,0,0,0]
		mr[m][0]+=x*x
		mr[m][1]+=x*y
		mr[m][2]+=y*y
		mr[m][3]+=1
		mr[m][4]+=x
		mr[m][5]+=y
		if x == 0:
			mr[m][6]+=1
			mr[m][7]+=y

ds={}
for line in open("date.txt"):
	cols=line.strip().split()
	if len(cols) < 2:
		continue
	ds[cols[0]]=int(cols[1])

def satisDegree(action):
	v=len(action.sons)
	n=1.0+0.5*len(action.pull)
	return math.log(1.0+v)/math.log(1.0+n)
	
#
#计算用户的方法调用在不同日期之间的关联
#
def outputMc(lastU,actions):
	mc1={}
	mc2={}
	for action in actions:
		method=action.method
		if action.method == "search.getlist":
			scene=column.getValue(action.para,"scene")
			if scene != "":
				method=method+"("+scene+")"
		try:
			date=TimeUtil.getDay(int(action.time))
		except:
			sys.stderr.write(action.time+"\n")
			continue
		if date not in ds:
			continue
		if ds[date] == 1:
			if action.method in tops:
				if method not in mc1:
					mc1[method]=[0,0,0]
				mc1[method][0]+=1
				sat=satisDegree(action)
				mc1[method][1]+=sat
				if sat > mc1[method][2]:
					mc1[method][2]=sat
		else:
			mc2[method]=1
	addUser(mc1,mc2)
	
def output(lastU,lastUser,actions):
	if len(sys.argv) >= 2:
		outputSeqStr(lastU,lastUser,actions)
	else:
		outputMc(lastU,actions)

#
#
#计算特定用户的行为在不同日期之间的关联
#或者打印这些用户行为序列------在output中分支
#
def printSeq(f):
	us=readUsers()
	lastU=""
	lastUser=None
	actions=[]
	userinfo=UserInfo("")
	for line in f:
		cols=line.strip().split("\t")
		u=cols[0]
		if u not in us:
			continue
		if cols[1] == "-1":
			userinfo=UserInfo.readUserInfo(cols)
			continue
		if lastU == "":
			lastU=u
			lastUser=userinfo
		if lastU != u:
			output(lastU,lastUser,actions)
			lastU=u
			lastUser=userinfo
			actions=[]
		action=Action.readAction(cols,userinfo)
		if action == None:
			continue
		actions.append(action)

	if lastU != "":
		output(lastU,lastUser,actions)
	
	for m in mr:
		xx=mr[m][0]
		xy=mr[m][1]
		yy=mr[m][2]
		n=mr[m][3]
		x=mr[m][4]
		y=mr[m][5]
		rx=mr[m][6]
		rxy=mr[m][7]
		x1=float(x)/(n+1e-32)
		y1=float(y)/(n+1e-32)
		print "%s\t%f\t%d\t%f\t%f\t%d\t%d\t%d\t%d\t%f\t%f\t%f"%(m,xx,yy,xy,x,y,n,rx,rxy,rxy/(rx+1e-12),xy/(xx+1e-12),(xy-n*x1*y1)/(math.pow(xx+1e-12-n*x1*x1,0.5)*math.pow(yy+1e-12-n*y1*y1,0.5)))


if __name__=="__main__":
	printSeq(sys.stdin)

