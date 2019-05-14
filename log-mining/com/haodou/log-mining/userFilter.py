#encoding=utf-8
#过滤该用户过去已经浏览过的菜谱

#两列是浏览记录
#三列是推荐候选
#
#将出现在浏览击落中的推荐候选去掉
#

import sys
sys.path.append("./")

import userSample

rinfo=userSample.readR()


def output(vs,rs,lastU):
	for r in vs:
		if r in rs:
			del rs[r]
	userSample.output(rs,lastU,rinfo)
	#for r in rs:
		#print lastU+"\t"+r+"\t"+str(rs[r])

lastU=""
vs={}
rs={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	if len(cols) < 2:
		continue
	u=cols[0]
	if lastU == "":
		lastU=u
	if lastU != u:
		output(vs,rs,lastU)
		lastU=u
		vs={}
		rs={}
	if len(cols) == 2:
		vs[cols[1]]=1
	else:
		rid=cols[1]
		if rid in rinfo:
			rs[rid]=float(cols[2])*rinfo[rid][1]

if lastU != "":
	output(vs,rs,lastU)


