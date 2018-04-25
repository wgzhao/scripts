import sys

cs={}
chs={}
hcs={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	c=cols[0]
	if len(cols) == 5:
		h=cols[1]
		v=int(cols[2])
		r=int(cols[3])
		if c not in chs:
			chs[c]={}
		if h not in chs[c]:
			chs[c][h]=[0,0]
		chs[c][h][0]+=v
		chs[c][h][1]+=r
		if h not in hcs:
			hcs[h]={}
		if c not in hcs[h]:
			hcs[h][c]=[0,0]
		hcs[h][c][0]+=v
		hcs[h][c][1]+=r
	else:
		v=int(cols[1])
		r=int(cols[2])
		if c not in cs:
			cs[c]=[0,0]
		cs[c][0]+=v
		cs[c][1]+=r
if sys.argv[1] == "c":
	for c in cs:
		print "%s\t%d\t%d\t%f"%(c,cs[c][0],cs[c][1],float(cs[c][0])/(1e-16+cs[c][1]))
elif sys.argv[1] == "ch":
	for c in chs:
		for h in chs[c]:
			print "%s\t%s\t%d\t%d\t%f"%(c,h,chs[c][h][0],chs[c][h][1],float(chs[c][h][0])/(1e-16+chs[c][h][1]))

elif sys.argv[1] == "hc":
	for h in hcs:
		for c in hcs[h]:
			print "%s\t%s\t%d\t%d\t%f"%(h,c,hcs[h][c][0],hcs[h][c][1],float(hcs[h][c][0])/(1e-16+hcs[h][c][1]))

