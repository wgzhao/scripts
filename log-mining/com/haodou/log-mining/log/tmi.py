import sys

sys.path.append("./")
sys.path.append("../")

from column import *

ms=None
if len(sys.argv) >= 2:
	import readParaConf
	ms=readParaConf.readParaSetting(sys.argv[1])

def addOne(prefix,m,u,nums):
	if m != None and len(m) > 0:
		m=prefix+m
		if ms != None and m not in ms:
			return
		print m+"\t"+u
		if m in nums:
			nums[m]+=1
		else:
			nums[m]=1

nums={}
for line in sys.stdin:
	cols=line.split("\t")
	if len(cols) < APP_LOG_COLUMNS:
		continue
	u=uuid(cols)
	if u == None or u == "":
		u="null"
	method=cols[METHOD_CID]
	m=readPara(cols,"recipe.getcollectrecomment","type")
	offset=readPara(cols,"recipe.getcollectrecomment","offset")
	if offset == "0":
		addOne("",m,u,nums)
