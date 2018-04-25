#encoding=utf-8

import sys

def output(lastW,cs,wc):
	maxC=""
	max=0
	sum=0
	d2=0
	for c in cs:
		if cs[c] > max:
			maxC=c
			max=cs[c]
		sum+=cs[c]
	for c in cs:
		d2+=float(cs[c])/sum*float(cs[c])/sum
	#print "%s\t%s\t%d\t%d\t%.3f\t%.3f"%(lastW,maxC,max,sum,max/float(sum),d2)
	wc[lastW]=(maxC,max,sum,d2)

def one(f,wc):
	lastW=""
	cs={}
	for line in f:
		cols=line.strip().split("\t")
		w1=cols[0]
		w2=cols[1]
		if len(cols) >= 3:
			v=float(cols[2])
		else:
			v=1.0
		if lastW == "":
			lastW=w1
		if lastW != w1:
			output(lastW,cs,wc)
			lastW=w1
			cs={}
		c=w2
		if w2 in wc:
			c=wc[w2][0]
		if c not in cs:
			cs[c]=1
		else:
			cs[c]+=1

if __name__=="__main__":
	wc={}
	for i in range(5):
		one(open("/home/zhangzhonghui/data/rsw1.txt"),wc)
	#for w in wc:
	#	(maxC,max,sum,d2)=wc[w]
	#	print "%s\t%s\t%d\t%d\t%.3f\t%.3f"%(w,maxC,max,sum,max/float(sum),d2)
	cw={}
	for w in wc:
		(c,max,sum,d2)=wc[w]
		if c not in cw:
			cw[c]={}
		cw[c][w]=(max,d2)
	for c in cw:
		for w in cw[c]:
			(max,d2)=cw[c][w]
			print "%s\t%s\t%d\t%.3f"%(c,w,max,d2)

