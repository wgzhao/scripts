#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../")

from column import *
import dictInfo

def count():
	v3c={}
	v4c={}
	for line in sys.stdin:
		line=line.strip()
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		method=cols[METHOD_CID]
		if method == "search.getcatelistv3":
			print line
			continue
		if not method.startswith("search.getlistv3"):
			continue
		version=bigVersion(cols[VERSION_CID])
		keyword=getValue(cols[PARA_ID],"keyword")
		tagid=""
		if keyword == None or keyword == "":
			tagid = getValue(cols[PARA_ID],"tagid")
			if tagid != "":
				pass
			else:
				typeid=getValue(cols[PARA_ID],"typeid")
				if typeid != "":
					print line

def action():
	for line in sys.stdin:
		try:
			a=eval(line.strip())
		except:
			continue
		if "ext" in a and "action"in a["ext"]:
			print a["b"]["a"],a["ext"]["page"],a["ext"]["action"]

def appleUserId():
	right=0
	wrong=0
	did=""
	pds={}
	ds={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) <= APP_LOG_COLUMNS:
			continue
		did=cols[UUID_ID]
		if did == "haodou" or did == "" or did == "null" or did == "haodoua1000033d709ec6":
			continue
		ip=cols[IP_CID]
		if cols[MEDIA_CID] == "appstore":
			uid=uuid(cols)
			if uid == None:
				continue
			if PositiveNumP.match(uid):
				if PositiveNumP.match(cols[USER_CID]):
					right+=1
					print line
				else:
					wrong+=1
			if did not in pds:
				pds[did]={}
			if ip not in pds[did]:
				pds[did][ip]=1
		else:
			if did not in ds:
				ds[did]={}
			if ip not in ds[did]:
				ds[did][ip]=1
	for did in pds:
		pds[did]=len(pds[did])
	for did in ds:
		ds[did]=len(ds[did])
	import dictInfo
	print dictInfo.info(ds)
	print dictInfo.info(pds)
	print "right",right
	print "wrong",wrong


def checkQid():
	qid="1408377611075095000"
	qid="1408393110001786000"
	for line in sys.stdin:
		if line.find(qid) > 0:
			print line

def checkReturnQid():
	qids={}
	mm={}
	for line in sys.stdin:
		qid=getValue(line,"request_id")
		if qid == None or len(qid) == 0:
			continue
		cols=line.strip().split("\t")
		if len(cols) < METHOD_CID+1:
			continue
		method=cols[METHOD_CID]
		if method != "":
			qids[qid]=method
		rqid=getValue(line,"return_request_id")
		if rqid != None and rqid != "":
			if rqid in qids:
				rmd=qids[rqid]
				if rmd not in mm:
					mm[rmd]={}
				if method not in mm[rmd]:
					mm[rmd][method]=1
				else:
					mm[rmd][method]+=1
	for m in mm:
		print m
		print "\t",mm[m]

def tagidDis():
	rkt={}
	kt={}
	t={}
	knt={}
	ks={}
	names={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < METHOD_CID+1:
			continue
		v=cols[VERSION_CID]
		if v != "4.0" and v != "v4.0":
			continue
		method=cols[METHOD_CID]
		if not method.startswith("search.getlist"):
			continue
		para=cols[PARA_ID]
		tid=getValue(para,"tagid")
		kw=getValue(para,"keyword")
		rqid=getValue(para,"return_request_id")
		if tid == "null":
			if kw != None and kw != "":
				if kw not in knt:
					knt[kw]=1
				else:
					knt[kw]+=1
		elif tid == "" or tid == None:
			if kw not in ks:
				ks[kw]=1
			else:
				ks[kw]+=1
		if tid != None and tid != "" and tid != "null":
			if kw != None and kw != "":
				if rqid != None and rqid != "":
					if tid not in rkt:
						rkt[tid]=1
					else:
						rkt[tid]+=1
				if tid not in kt:
					kt[tid]=1
				else:
					kt[tid]+=1
				if tid not in names:
					names[tid]=kw
			else:
				if tid not in t:
					t[tid]=1
				else:
					t[tid]+=1
	print "len of rkt",len(rkt)
	print "len of kt",len(kt)
	print "len of t",len(t)
	for tid in rkt:
		if tid not in names:
			names[tid]=""
		print tid,names[tid],rkt[tid]
	for tid in t:
		if tid in names:
			print tid,names[tid],t[tid]
		else:
			print tid,t[tid]
	for tid in kt:
		print "kt",tid,names[tid],kt[tid]
	info=dictInfo.info(knt)
	print info
	for k in info["top10"]:
		print k[0],k[1]
	info=dictInfo.info(ks)
	print info
	for k in info["top10"]:
		print k[0],k[1]

def ipDis():
	ips={}
	lastIp={}
	wrong=0
	total=0
	ws={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		if line.find("mobiledevice.initandroiddevice") > 0:
			continue
		u=ActionUser(cols)
		if u == None or u=="":
			continue
		ip=cols[IP_CID]
		if ip == "":
			continue
		if ip not in ips:
			ips[ip]=u
		else:
			v=ips[ip]
			if type(v) == dict:
				if u not in v:
					v[u]=1
				else:
					if u != lastIp[ip]:
						wrong+=1
						if ip not in ws:
							ws[ip]=1
						else:
							ws[ip]+=1
			else:
				if v != u:
					ips[ip]={}
					ips[ip][u]=1
					ips[ip][v]=1
		lastIp[ip]=u
		total+=1
	for ip in ips:
		v=ips[ip]
		if type(v) == dict:
			ips[ip]=len(v)
		else:
			ips[ip]=1
	print dictInfo.info(ws)	
	print "total",total
	print "wrong",wrong
	print dictInfo.info(ips)

def readMLog():
	for line in sys.stdin:
		a=eval(line.strip())
		print a["remote_addr"]

def groupUsers():
	escape=0
	if len(sys.argv) >= 2:
		escape=int(sys.argv[1])
	tm=None
	if len(sys.argv) >= 3:
		tm=sys.argv[2]
	ei=0
	us={}
	N=0
	for line in sys.stdin:
		if N%100000 == 0:
			for u in us:
				if len(us[u]) < 10:
					continue
				ei+=1
				if ei <= escape:
					continue
				has=True
				if tm != None:
					has=False
					for s in us[u]:
						if s.find(tm) > 0:
							has=True
				if has:
					for s in us[u]:
						print s
					print "\n"
			us={}
		cols=line.strip().split("\t")
		u=ActionUser(cols)
		if u == None or u == "":
			continue
		if u not in us:
			us[u]=[]
		us[u].append(line.strip())
		N+=1

def isFavType():
	ds={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		method=cols[METHOD_CID]
		if method == "favorite.isfav":
			type=getValue(cols[PARA_ID],"type")
			v=bigVersion(cols[VERSION_CID])
			tv=v+"_"+type
			if tv not in ds:
				ds[tv]=1
			else:
				ds[tv]+=1
	for tv in ds:
		print tv,ds[tv]

def methodDistribute():
	ms={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		method=cols[METHOD_CID]
		if method not in ms:
			ms[method]=1
		else:
			ms[method]+=1
	for m in ms:
		print m,ms[m]
	print dictInfo.info(ms)
	
def testVersionForInitDevice():
	vs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS:
			continue
		method=cols[METHOD_CID]
		if method == "mobiledevice.initandroiddevice":
			version=cols[VERSION_CID]
			if version not in vs:
				vs[version]=1
			else:
				vs[version]+=1
	for v in vs:
		print v,vs[v]

def urlEncode():
	sys.path.append("../abtest")
	import column2
	from urllib import urlencode
	from urllib import unquote
	for line in sys.stdin:
		(message,endPos)=column2.getValue(line,'"message":"','"',0)
		if message != None:
			print unquote(message)
			print column2.escapeUnicode(message).encode("utf-8")
			print line

def pushHit():
	import TimeUtil
	import os
	ds=TimeUtil.days("2014-07-01","2014-08-28")
	name="pushview"
	name="push_received"
	for d in ds:
		estr='echo '+'"'+d+'\t"$('+'hdfs dfs -text  /online_logs/beijing/behaviour/'+name+'/'+d+'/'+name+'.log.'+d+".lzo 2> /dev/null | wc | awk '{print $1}')"
		#print estr
		os.system(estr)

def getAlumId():
	sys.path.append("../abtest")
	import column2
	aids={}
	for line in sys.stdin:
		ret=column2.getRecoms(line)
		if ret != None and "album" in ret:
			id=ret["album"][0][0]
			print id
	print aids
			#[0],column2.escapeUnicode(ret["album"][1]).encode("utf-8")


def view():
	qs={}
	qs['4fbc578cd114dd0a3bf4a94864cfe987']=1
	for line in sys.stdin:
		for q in qs:
			if line.find(q) > 0:
				print line

def testMatch():
	import queryTag
	word=u"无敌懒人菜大公开，再也不怕闷热厨房啦！"
	word=u"吃fun20:给胖子一条死路"
	tags={}
	queryTag.match(word,tags,0,len(word),True)
	for sub in tags:
		print sub[1].encode("utf-8")
		for tag in tags[sub]:
			print tag.encode("utf-8")

def getPushUser():
	lastU=""
	ts={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		u=cols[0]
		if lastU == "":
			lastU=u
		if lastU != u:
			if "[婴儿]" not in ts and "[儿童]" in ts:
				print u
			lastU=u
			ts={}
		t=cols[1]
		ts[t]=1
	if lastU != "":
		if "[婴儿]" not in ts and "[儿童]" in ts:
			print u

def samplePushUser(n):
	total=0
	list=[]
	for line in sys.stdin:
		list.append(line.strip())
	rate=float(n)/float(len(list))
	sys.stderr.write("%d\t%d\t%f"%(int(n),len(list),rate)+"\n")
	import random
	for line in list:
		r=random.random()
		if r <= rate:
			print line
		elif r <= 2*rate:
			print line+"\t1"

def checkLen():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		v=column.bigVersion(cols[VERSION_CID])
		if v == "2" and len(cols) > column.APP_LOG_COLUMNS:
			print line

def testWeekDay():
	import datetime
	time=1406691184
	time=1405400145
	print datetime.datetime.fromtimestamp(time).weekday()

if __name__=="__main__":
	#count()
	#action()
	#appleUserId()
	#checkQid()
	#checkReturnQid()
	#tagidDis()
	#ipDis()
	#readMLog()
	#isFavType()
	#groupUsers()
	#methodDistribute()
	#testVersionForInitDevice()
	#urlEncode()
	#pushHit()
	#getAlumId()
	view()
	#testMatch()
	#getPushUser()
	#samplePushUser(sys.argv[1])
	#checkLen()
	#testWeekDay()
