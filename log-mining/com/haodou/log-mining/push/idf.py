import sys

DocNumTag="#all#"

def readStopTag(f):
	stops={}
	for line in f:
		stops[line.strip()]=1
	return stops

def countIdf():
	df={}
	stops=readStopTag(open("stopTag.txt"))
	df[DocNumTag]=0
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		for i in range(2,len(cols),1):
			t=cols[i]
			if t in stops:
				continue
			if t not in df:
				df[t]=1
			else:
				df[t]+=1
		df[DocNumTag]+=1
	for t in df:
		print "%s\t%d"%(t,df[t])

def reduce():
	lastT=""
	n=0
	for line in sys.stdin:
		cols=line.strip().split("\t")
		t=cols[0]
		if lastT == "":
			lastT=t
		if lastT != t:
			if n >= 2:
				print "%s\t%d"%(lastT,n)
			lastT=t
			n=0
		n+=int(cols[1])
	if lastT != "":
		if n >= 2:
			print "%s\t%d"%(lastT,n)

import math
def readIdf(cut=2):
	d={}
	for line in open("df.txt"):
		cols=line.strip().split("\t")
		t=cols[0]
		n=int(cols[1])
		if n >= cut:
			d[t]=n
	allNum=d[DocNumTag]
	for t in d:
		d[t]=math.log(allNum/d[t])
	return d

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "map":
			countIdf()
		elif sys.argv[1] == "reduce":
			reduce()
	else:
		d=readIdf()
		for t in d:
			print t,d[t]

