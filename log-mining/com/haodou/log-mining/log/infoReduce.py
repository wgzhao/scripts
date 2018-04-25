import sys

sys.path.append("./")
from dictInfo import *

def output(lastm,us):
	s=lastm
	print lastm+"\t"+str(info(us))

if __name__=="__main__":
	lastm=""
	us={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		m=cols[0]
		if lastm == "":
			lastm=m
		if lastm != m:
			output(lastm,us)
			lastm=m
			us={}
		if len(cols) < 2:
			continue
		u=cols[1]
		if u not in us:
			us[u]=1
		else:
			us[u]+=1

	if lastm != "":
		output(lastm,us)

