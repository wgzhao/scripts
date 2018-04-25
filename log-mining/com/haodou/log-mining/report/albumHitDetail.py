import sys

sys.path.append("../log")
import clickCount


albumHit={}
albumShow={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	show=clickCount.readAttribute(line,"albumShow")
	if show == None:
		continue
	k=cols[0]
	if k.startswith("v45_"):
		k=k[4:]
	for id in show:
		if id == clickCount.SumMark:
			continue
		v=show[id]
		if k not in albumShow:
			albumShow[k]=v
		else:
			albumShow[k]+=v
	hit=clickCount.readAttribute(line,"albumHit")
	if hit == None:
		continue
	for id in hit:
		if id == clickCount.SumMark:
			continue
		v=hit[id]
		if k not in albumHit:
			albumHit[k]=v
		else:
			albumHit[k]+=v

for k in albumShow:
	sv=albumShow[k]
	tv=0
	if k in albumHit:
		tv=albumHit[k]
		if sv >= 100:
			print "%s\t%d\t%d\t%.3f"%(str(k),tv,sv,float(tv)/(sv+1e-12))


	

