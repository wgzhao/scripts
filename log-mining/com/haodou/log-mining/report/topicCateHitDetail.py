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
	k=cols[0]
	if k.startswith("v45_"):
		k=k[4:]
	for id in topicShow:
		if id == clickCount.SumMark:
			continue
		if id in tc:
			t=tc[id]
			v=topicShow[id]
			if t not in cateShow:
				cateShow[t]={}
			if k not in cateShow[t]:
				cateShow[t][k]=v
			else:
				cateShow[t][k]+=v
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
				cateHit[t]={}
			if k not in cateHit[t]:
				cateHit[t][k]=v
			else:
				cateHit[t][k]+=v

for t in cateShow:
	sd=cateShow[t]
	td={}
	if t in cateHit:
		td=cateHit[t]
	for id in sd:
		sv=sd[id]
		tv=0
		if id in td:
			tv=td[id]
		if sv >= 100:
			print "%d\t%s\t%d\t%d\t%.3f"%(t,str(id),tv,sv,float(tv)/(sv+1e-12))


	

