import sys

sys.path.append("./")

import math
from CollectionUtil import *

UNum=100000
RNum=1000

def output(rs,us,lastTag):
	N=len(rs)/2+5
	if N > RNum:
		N=RNum
	cut(rs,N)
	N=len(us)/2+5
	if N > UNum:
		N=UNum
	cut(us,N)
	for rid in rs:
		for uid in us:
			print uid+"\t"+rid+"\t"+str(math.log(rs[rid]+1.0)*math.log(us[uid]+1.0))

us={}
rs={}
lastTag=""
for line in sys.stdin:
	cols=line.strip().split("\t")
	tag=cols[0]
	try:
		v=float(cols[2])
	except:
		sys.stderr.write(line)
		continue
	if lastTag == "":
		lastTag=tag
	if lastTag != tag:
		output(rs,us,lastTag)
		lastTag=tag
		rs={}
		us={}
	if len(cols) == 4:
		u=cols[1]
		if u not in us:
			us[u]=0
		us[u]+=float(cols[2])
	else:
		rs[cols[1]]=float(cols[2])
if lastTag != "":
	output(rs,us,lastTag)


