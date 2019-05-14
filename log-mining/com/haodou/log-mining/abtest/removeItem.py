#encoding=utf-8

import sys

sys.path.append("./")

NeedKey="__needkey__"

def readRemoveItems(f):
	removes={}
	for line in f:
		cols=line.strip().split()
		if len(cols) <= 0:
			continue
		head=cols[0]
		hs=head.split(".")
		tmp=removes
		for h in hs:
			if h not in tmp:
				tmp[h]={}
			tmp=tmp[h]
		if len(cols) >= 2:
			if NeedKey not in tmp:
				tmp[NeedKey]=[]
			tmp[NeedKey].append(cols[1])
	return removes

def parse(line):
	line=line.strip()
	if len(line) == 0:
		return None
	if line.startswith("#"):
		return None
	try:
		line="False".join(line.split("false"))
		line="True".join(line.split("true"))
		line="None".join(line.split("null"))
		a=eval(line)
		return a
	except ValueError:
		return line

def removeSub(ta,tremoves,removes):
	if type(ta) == list:
		for mi in ta:
			if type(mi) != dict:
				continue
			removeDict(mi,tremoves,removes)
	elif type(ta) == dict:
		removeDict(ta,tremoves,removes)		

def removeDict(d,r,removes):
	needs={}
	if NeedKey in r:
		for k in r[NeedKey]:
			needs[k]=1
	ks=[]
	hasNeeds={}
	for k in d:
		ks.append(k)
		if k in needs:
			hasNeeds[k]=1
	if len(hasNeeds) > 0:
		for k in ks:
			if k not in hasNeeds:
				del d[k]
			else:
				if k in r:
					removeSub(d[k],r[k],removes)
	else:
		for k in ks:
			if k not in r:
				if k in removes and len(removes[k]) == 0:
					del d[k]
				else:
					removeSub(d[k],removes,removes)
				continue
			if len(r[k]) == 0:
				del d[k]
			elif len(r[k]) > 0:
				removeSub(d[k],r[k],removes)

def remove(line,removes):
	a=parse(line)
	if a != None and type(a) != str:
		removeSub(a,removes,removes)
		return a
	else:
		return None

def testRemove():
	removes=readRemoveItems(open("removedItem.txt"))
	for line in sys.stdin:
		a=remove(line,removes)
		if a != None:
			print a
		else:
			print line.strip()

if __name__=="__main__":
	testRemove()

