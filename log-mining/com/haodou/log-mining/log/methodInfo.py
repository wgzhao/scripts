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

def appendBool(n,v):
	if v != None and v != "" and v != "null":
		return n+"_"
	else:
		return "_"

nums={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	if len(cols) < APP_LOG_COLUMNS:
		continue
	u=uuidFirst(cols)
	if u == None or u == "":
		u="null"
	version=bigVersion(cols[VERSION_CID])
	addOne("version_",version,u,nums)
	method=cols[METHOD_CID]+"_"+version
	addOne("",method,u,nums)
	if method.startswith("search.getlist"):
		keyword=getValue(cols[PARA_ID],"keyword")
		return_request_id=getValue(cols[PARA_ID],"return_request_id")
		tagid=getValue(cols[PARA_ID],"tagid")
		typeid=getValue(cols[PARA_ID],"typeid")
		t=""
		t+=appendBool("keyword",keyword)
		t+=appendBool("rqid",return_request_id)
		t+=appendBool("tagid",tagid)
		t+=appendBool("typeid",typeid)
		addOne(method+"_",t,u,nums)
		offset=getValue(cols[PARA_ID],"offset")
		if offset == "0":
			addOne("c0"+method+"_",t,u,nums)
	elif method.startswith("wiki.getlistbytype"):
		m=getValue(cols[PARA_ID],"type")
		addOne(method+"_",m,u,nums)
	elif method.startswith("recipephoto.getproducts"):
		m=getValue(cols[PARA_ID],"type")
		if m == "1":
			m=getValue(cols[PARA_ID],"id")
			addOne(method+".id_",m,u,nums)
	elif method.startswith("rank.getrankview"):
		m=getValue(cols[PARA_ID],"id")
		addOne(method+".id_",m,u,nums)
	elif method.startswith("recipe.getcollectrecomment"):
		m=getValue(cols[PARA_ID],"type")
		addOne("",m,u,nums)
		offset=getValue(cols[PARA_ID],"offset")
		if offset == "0":
			addOne("c0",m,u,nums)
	elif method.startswith("topic.getlist"):
		m=getValue(cols[PARA_ID],"cate_id")
		addOne(method+"_",m,u,nums)
	elif method.startswith("favorite.isfav"):
		m=getValue(cols[PARA_ID],"type")
		addOne(method+"_",m,u,nums)
