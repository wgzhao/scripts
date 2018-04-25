import sys
sys.path.append("./")

import math

def output(tf,rid):
	for corid in tf:
		s=rid+"\t"+corid+"\t"+str(tf[corid])
		print s

def weit():
	lastRid=""
	tf={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			sys.stderr.write(line)
			continue
		rid=cols[0]
		corid=cols[1]
		if lastRid == "":
			lastRid=rid
		if lastRid != rid:
			output(tf,lastRid)
			lastRid=rid
			tf={}
		if corid not in tf:
			tf[corid]=0
		c=1.0
		if len(cols) >= 3:
			c=float(cols[2])
		tf[corid]+=c

	if lastRid != "":
		output(tf,lastRid)

if __name__=="__main__":
	weit()

