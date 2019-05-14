#encoding=utf-8
import sys

TIME_CID=0
DEVICE_CID=1
MEDIA_CID=2
IP_CID=4
APPID_CID=5
VERSION_CID=6
USER_CID=7
METHOD_CID=8
PARA_ID=9
UUID_ID=10

APP_LOG_COLUMNS=10
APP_LOG_COLUMNS4=11

import re
PositiveNumP=re.compile("[1-9][0-9]*")

import datetime

def escapeUtf8(v):
	if v.find("\u") >= 0:
		while v.endswith("\\"):
			v=v[0:-1]
		v=eval('u'+'"'+v+'"').encode("utf-8")
	return v

def getHour(time):
	return datetime.datetime.fromtimestamp(time).hour

def getWeekDay(time):
	return datetime.datetime.fromtimestamp(time).weekday()

def detailMethod(method,para):
	if method == "search.getlist":
		scene=getValue(para,"scene")
		if scene != "":
			method=method+"-"+scene
	return method

def uid(cols):
	if len(cols) < APP_LOG_COLUMNS:
		return None
	u=cols[USER_CID]
	if PositiveNumP.match(u):
		return u
	u=getValue(cols[PARA_ID],"uid")
	if PositiveNumP.match(u):
		return u
	return ""

def uuid(cols):
	if len(cols) < APP_LOG_COLUMNS:
		return None
	u=cols[USER_CID]
	if PositiveNumP.match(u):
		return u
	u=getValue(cols[PARA_ID],"uid")
	if PositiveNumP.match(u):
		return u
	if len(cols) >= UUID_ID+1:
		return cols[UUID_ID]
	u=getValue(cols[PARA_ID],"uuid")
	if len(u) > 0:
		return u
	u=cols[DEVICE_CID]
	if u.find("null") >= 0 or len(u) == 0:
		u=cols[IP_CID]
	if u == "":
		u=None
	return u

def uuidOnly(cols):
	if len(cols) >= UUID_ID+1:
		u=cols[UUID_ID]
		if u.find("{") < 0 and len(u) <= 64:
			return u
	if len(cols) >= APP_LOG_COLUMNS:
		u=getValue(cols[PARA_ID],"uuid")
		if len(u) > 0 and len(u) <= 64:
			return u
	return None

def uuidFirst(cols):
	id=uuidOnly(cols)
	if id != None:
		return id
	return uuid(cols)

def ActionUser(cols):
	if len(cols) < APP_LOG_COLUMNS:
		return None
	if len(cols) >= UUID_ID+1:
		return cols[UUID_ID]
	u=cols[DEVICE_CID]
	if u.find("null") >= 0 or len(u) == 0:
		u=u=cols[IP_CID]
	if u == "":
		return None
	return u


def user(cols):
	if len(cols) < APP_LOG_COLUMNS:
		return None
	u=cols[USER_CID]
	if len(u) == 0 or u == '0':
		u=cols[DEVICE_CID]
		if u.find("null") >= 0:
			u=""
	if u == "":
		u=getValue(cols[PARA_ID],"uid")
	if u == "":
		u=None
	return u

import re
ridP=re.compile(r"^[0-9]+$")
def getRid(cols):
	rid=getValue(cols[PARA_ID],"rid")
	if rid == "":
		rid=getValue(cols[PARA_ID],"recipeid")
	if not ridP.match(rid):
			return ""
	return rid

aidP=re.compile(r"^[0-9]+$")
def getAid(cols):
	aid=getValue(cols[PARA_ID],"aid")
	if not aidP.match(aid):
		return ""
	return aid

tidP=re.compile(r"^[0-9]+?$")
def checkTid(tid):
	if tid.endswith(".html"):
		tid=tid[0:-5]
	if not tidP.match(r"^[0-9]+?$"):
		return ""
	return tid

pidP=re.compile(r"^[0-9]+$")
def getPid(cols):
	pid=getValue(cols[PARA_ID],"pid")
	if not pidP.match(pid):
		return ""
	return pid

def getTag(cols):
	if len(cols) < APP_LOG_COLUMNS:
		return None
	method=cols[METHOD_CID]
	rid=getRid(cols)
	if rid != "":
		rid="rid-"+rid
		if method.find("info") >= 0:
			return (rid,1)
		if method.find("isfav") >= 0:
			return (rid,3)
	if method.find("search") >= 0:
		keyword=getValue(cols[PARA_ID],"keyword")
		keyword=keyword.replace("\t","")
		keyword=keyword.replace("\n","")
		keyword=keyword.replace("\r","")
		if keyword != "":
			return (keyword,3)
		typeid=getValue(cols[PARA_ID],"typeid")
		if typeid == "":
			typeid=getValue(cols[PARA_ID],"tagid")
		if typeid != "":
			return ("typeid-"+typeid,2)
	return None

def getValue(para,key):
	key='"'+key+'"'
	tpos=para.find(key)
	if tpos < 0:
		return ""
	if len(para) > tpos+len(key) and para[tpos+len(key)] == ";":
		pos=para.find(":",tpos+len(key)+4)+2
	else:
		pos=para.find(":",tpos+len(key))+2
	if pos < tpos:
		return ""
	pos1=para.find(';',pos)
	if pos1 < pos:
		pos1=para.find('"',pos+1)
		if pos1 > pos:
			s=para[pos:pos1]
			return escapeUtf8(s)
	if pos1 < pos:
		return ""
	return para[pos:pos1-1]



#qidP=re.compile("^[0-9]{19}$")
#近期qid日志的格式发生变化了，不一定是数字
#

def getQid2(para,key):
	key='"'+key+'":"'
	pos=para.rfind(key)
	if pos < 0:
		return None
	pos1=para.find('"',pos+len(key))
	if pos1 > pos:
		qid=para[pos+len(key):pos1]
		return qid
	return None

def getValueNginx(para,key):
	key='"'+key+'":"'
	pos=para.rfind(key)
	if pos < 0:
		return None
	pos1=para.find('"',pos+len(key))
	if pos1 > pos:
		return para[pos+len(key):pos1]
	return None

def bigVersion(v):
	if len(v) == 0:
		return v
	if v.startswith("v"):
		v=v[1:]
	return v[0]

def intVersion(v):
	try:
		if len(v) == 0:
			return 0
		if v.startswith("v"):
			v=v[1:]
		v="".join(v.split("."))
		v=int(v)
		if v >= 100:
			return v
		elif v >= 10:
			return v*10
		else:
			return v*100
	except:
		return 0	

def getSearchType(method,para):
	scene=getValue(para,"scene")
	if scene != "":
		return scene
	tag=getValue(para,"tagid")
	if tag == "":
		tag=getValue(para,"typeid")
	if tag != "" and tag != "null":
		return "t"
	keyword=getValue(para,"keyword")
	if keyword != "":
		return "k"
	return "v"

def columns(line,deli="\t"):
	cols=line.strip().split(deli)
	return cols

def readPara(cols,methodName,paraName):
	if len(cols) <= METHOD_CID or len(cols) <= PARA_ID:
		return ""
	if cols[METHOD_CID] != methodName:
		return ""
	return getValue(cols[PARA_ID],paraName)

QUERY_MTHOED="search.getlistv3"
QUERY_PARA="keyword"

def readQuery(f):
	qs={}
	for line in f:
		cols=columns(line,"\t")
		query=readPara(cols,QUERY_MTHOED,QUERY_PARA)
		query=query.strip()
		if len(query) > 0:
			if query not in qs:
				qs[query]=0
			qs[query]+=1
	if __name__=="__main__":
		for q in qs:
			print q+"\t"+str(qs[q])

import re
TimeP=re.compile(r"^[1-9][0-9]{9}$")

def valid(cols):
	if len(cols) < APP_LOG_COLUMNS:
		return False
	if len(cols) == APP_LOG_COLUMNS:
		para=cols[APP_LOG_COLUMNS-1]
		if not para.endswith('";}') and not para.endswith('{}'):
			return False
	return True

def pre(f):
	lastQLine=""
	for line in f:
		line=line.strip()
		cols=line.split("\t")
		if not valid(cols):
			if lastQLine != "":
				line=lastQLine+line
				cols=line.split("\t")
			if not valid(cols):
				if TimeP.match(cols[0]):
					lastQLine=line
				continue
			lastQLine=""
		print line

def testUser():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if user(cols) == None:
			print line

def testGetTag():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		t=getTag(cols)
		if t!=None and t[0].startswith("t"):
			print line,"("+t[0]+",",t[1],")"

def testBigVersion():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		print bigVersion(cols[VERSION_CID])

def testActionUser():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=ActionUser(cols)
		print line.strip()
		print u
		print ""

def testIntVersion():
	print intVersion("v4.1")
	print intVersion("4.4.1")
	print intVersion("4.5.0")
	print intVersion("4.0")
	print intVersion("")
	print intVersion("_")
	print intVersion(0)

def testGetValue(para="keyword"):
	for line in sys.stdin:
		v=getValue(line,para)
		if v==None or len(v) == 0:
			continue
		print line
		print para,v

if __name__=="__main__":
	#readQuery(sys.stdin)
	#testUser()
	#testGetTag()
	#pre(sys.stdin)
	#testBigVersion()
	#testActionUser()
	#testIntVersion()
	testGetValue(sys.argv[1])
