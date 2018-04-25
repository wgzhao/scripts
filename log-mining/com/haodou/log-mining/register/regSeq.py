#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")
#import column

sys.path.append("./")
sys.path.append("../sequence")
sys.path.append("../util")
sys.path.append("../")

#import actionUserInfo

def readMethod(f):
	ms={}
	for line in f:
		line=line.strip()
		if line.startswith("#"):
			continue
		cols=line.split()
		if len(cols) < 1:
			continue
		ms[cols[0]]=1
	return ms

regMs=readMethod(open("regMethod.txt"))
regUserMs=readMethod(open("regUserMethod.txt"))

lms={}

def output(lastU,lastUser,ms):
	if len(ms) <= 0:
		return
	lm="NO"
	for m in ms:
		if m in regUserMs:
			lm=m
			break
	if lm not in lms:
		lms[lm]=1
	else:
		lms[lm]+=1
	#print lastU+"\t"+"_".join(ms)+"\t"+lm

def count():
	lastU=""
	lastUser=None
	user=None
	ms=[]
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 5:
			continue
		u=cols[0]
		time=cols[1]
		if time == "-1":
			user=None
			#user=actionUserInfo.UserInfo.readUserInfo(cols)
			#if lastU == u:
			#	lastUser.merge(user)
		if lastU == "":
			lastU=u
			lastUser=user
		if lastU != u:
			output(lastU,lastUser,ms)
			lastU=u
			lastUser=user
			ms=[]
		method=cols[3]
		if method in regMs:
			ms.append(method)
		elif method in regUserMs:
			if len(ms) >= 1 and ms[-1] in regMs:
				ms.append(method)
		
	if lastU != "":
		output(lastU,lastUser,ms)
	
	for lm in lms:
		print lm+"\t"+str(lms[lm])

def reduce():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			continue
		lm=cols[0]
		v=int(cols[1])
		if lm not in lms:
			lms[lm]=v
		else:
			lms[lm]+=v
	for lm in lms:
		print lm+"\t"+str(lms[lm])

if __name__=="__main__":
	if sys.argv[1] == "map":
		count()
	else:
		reduce()



