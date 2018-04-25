import sys

sys.path.append("../log")
import clickCount

tc={}
for line in open("/home/zhangzhonghui/data/topicCate.txt"):
	cols=line.strip().split("\t")
	tc[int(cols[0])]=int(cols[1])

cateHit={}
cateShow={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	topicShow=clickCount.readAttribute(line,"topicShow")
	if topicShow == None:
		continue
	for id in topicShow:
		if id == clickCount.SumMark:
			continue
		if id in tc:
			t=tc[id]
			v=topicShow[id]
			if t not in cateShow:
				cateShow[t]=v
			else:
				cateShow[t]+=v
	topicHit=clickCount.readAttribute(line,"topicHit")
	if topicHit == None:
		continue
	for id in topicHit:
		if id == clickCount.SumMark:
			continue
		if id in tc:
			t=tc[id]
			v=topicHit[id]
			if t not in cateHit:
				cateHit[t]=v
			else:
				cateHit[t]+=v

for t in cateShow:
	sv=cateShow[t]
	st=0
	if t in cateHit:
		st=cateHit[t]
	print "%d\t%d\t%d\t%.3f"%(t,st,sv,float(st)/(sv+1e-12))


	

