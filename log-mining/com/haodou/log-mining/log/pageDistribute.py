#encoding=utf-8

import sys

sys.path.append("./")
import cnameMap

prefix="recipe.getcollectrecomment_request--"

def count(f):
	cs={}
	for line in f:
		if not line.startswith(prefix):
			continue
		line=line[len(prefix):]
		cols=line.strip().split("\t")
		pos=cols[0].find("-")
		c=cols[0][0:pos]
		if len(c) == 0:
			continue
		c=cnameMap.map(c)
		page=cols[0][pos+1:]
		if len(page) <= 0:
			continue
		page=int(page)
		num=eval(cols[1])["userNum"]
		if c not in cs:
			cs[c]={}
		if page not in cs[c]:
			cs[c][page]=0
		cs[c][page]+=num
	rs={}
	for c in cs:
		sum=0.0
		slist=[(k,cs[c][k]) for k in sorted(cs[c].keys())]
		if len(slist) < 7:
			continue
		print c,slist
		n0=float(slist[0][1])
		for page,num in slist:
			sum+=num
		s=""
		tsum=0
		tsum6=0
		maxPage=0
		lost=False
		lastNum=1.0
		for page,num in slist:
			if num > 1 and num/lastNum > 0.01 and not lost:
				maxPage=page
			else:
				lost=True
			lastNum=float(num)
			tsum+=num
			if page < 50:
				tsum6 += num
			s+="\t("+str(page)+","+"%.3f"%(tsum/sum)+","+"%.3f"%(num/sum)+","+"%.3f"%(num/n0)+")"
		print s
		n1=float(slist[1][1]+slist[2][1])
		rs[c]=(n0/sum,n1/n0,tsum6/sum,n0/tsum6,n1/(tsum6-n0),tsum6,maxPage)
	print "栏目名\t首屏占比\t次屏比首屏数\t前5屏占比\t首屏占前5屏比重\t次屏占前5屏比（去首屏）\t前5屏刷屏数\t列表最大菜谱数"
	for c in rs:
		print "%s\t%.3f\t%.3f\t%.3f\t%.3f\t%.3f\t%d\t%d"%(c,rs[c][0],rs[c][1],rs[c][2],rs[c][3],rs[c][4],rs[c][5],rs[c][6])

if __name__=="__main__":
	count(sys.stdin)


