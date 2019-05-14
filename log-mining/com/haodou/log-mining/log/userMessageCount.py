import sys

def output(lastTag,np,nv,hp,hv):
	hrate=hv/(hp+hv+1e-6)
	nrate=nv/(np+nv+1e-6)
	div=hrate-nrate
	print "%s\t%d\t%d\t%d\t%d\t%d\t%f\t%f\t%f"%(lastTag,hv,hv+hp,nv,np+nv,hv+hp+np+nv,hrate,nrate,div)

def countTag():
	lastTag=""
	np=0
	nv=0
	hp=0
	hv=0
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		tag=cols[0]
		if lastTag == "":
			lastTag=tag
		if lastTag != tag:
			output(lastTag,np,nv,hp,hv)
			lastTag=tag
			np=0
			nv=0
			hp=0
			hv=0
		if cols[1] == "NOT":
			if cols[2] == "push":
				np+=1
			else:
				nv+=1
		else:
			if cols[2] == "push":
				hp+=1
			else:
				hv+=1
	if lastTag != "":
		output(lastTag,np,nv,hp,hv)

if __name__=="__main__":
	countTag()


