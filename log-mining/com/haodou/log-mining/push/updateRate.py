#encoding=utf-8

import sys

cs={}
for line in open("itemRate.old.txt"):
	cols=line.strip().split("\t")
	id=cols[0]
	v=float(cols[1])
	title=cols[2]
	cs[id]=[title,1000,int(1000*v)]

cs={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	id=cols[1]
	title=cols[2]
	r=int(cols[3])
	t=int(cols[4])
	if id not in cs:
		cs[id]=[title,r,t]
	else:
		cs[id][1]+=r
		cs[id][2]+=t

for id in cs:
	p=cs[id]
	title=p[0]
	r=p[1]
	t=p[2]
	v=t/(r+1e-32)
	print "%s\t%f\t%s\t%d\t%d"%(id,v,title,r,t)


