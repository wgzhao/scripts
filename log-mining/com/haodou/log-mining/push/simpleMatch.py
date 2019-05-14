#encoding=utf-8

import sys

sys.path.append("./")
import uuid

sys.path.append("../log")
import queryTag

import random

def matchOutput(lastU,cs):
	if len(cs) > 0:
		maxC=""
		max=0
		randomC=""
		rid=random.randint(0,len(cs)-1)
		id=0
		for c in cs:
			if cs[c] > max:
				max=cs[c]
				maxC=c
			if id == rid:
				randomC=c
			id+=1
		if random.random() > 0.5:
			print lastU+"\t"+maxC
		else:
			print lastU+"\t"+randomC

#input:4e2ef13568756247db8678ca861eaeba        豆腐    豆腐    [主食材]        [豆类]
def match(w2cs):
	lastU=""
	cs={}
	for line in sys.stdin:
		line = uuid.checkUuidLine(line)
		if line == None:
			continue
		cols=line.strip().split("\t")
		if cols[0].find(" ") > 0:
			continue
		u=cols[0]
		if lastU == "":
			lastU=u
		if lastU != u:
			matchOutput(lastU,cs)
			lastU=u
			cs={}
		for w in cols[1:]:
			if w in w2cs:
				for c in w2cs[w]:
					if c not in cs:
						cs[c]=1
					else:
						cs[c]+=1
	if lastU != "":
		matchOutput(lastU,cs)

def readW2c(file):
	w2cs={}
	for line in open(file):
		cols=line.strip().split()
		if line.strip().startswith("#"):
			continue
		if len(cols) < 1:
			continue
		w=cols[0]
		if w not in w2cs:
			w2cs[w]=[]
		if len(cols) == 1:
			if "" not in w2cs[w]:
				w2cs[w].append("")
		else:
			for c in cols[1:]:
				if c not in w2cs[w]:
					w2cs[w].append(c)
	return w2cs	

def getW2c(title,tag):
	tags=queryTag.getAllTags(title)	
	for t in tags:
		print t

def testMatch():
	w2cs=readW2c(sys.argv[1])
	match(w2cs)

if __name__=="__main__":
	testMatch()
	#getW2c("【15元起包邮】滋补养颜喝靓汤→","daojia_tang_20151204")


