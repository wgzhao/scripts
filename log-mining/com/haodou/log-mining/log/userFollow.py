import sys

sys.path.append("./")
sys.path.append("../")
sys.path.append("../abtest")

import column
import column2

def	readErase(f=open("erase.txt")):
	cs={}
	ers={}
	for line in f:
		cols=line.strip().split()
		if len(cols) < 2:
			continue
		head=cols[0]
		if head.startswith("###"):
			cs[head]={}
			for i in range(1,len(cols),1):
				w=cols[i].strip()
				if len(w) == 0:
					continue
				cs[head][w]=1
			for w in cs[head]:
				if w not in ers:
					ers[w]={w:1}
				for w1 in cs[head]:
					ers[w][w1]=1
		elif head.startswith("="):
			head=head[1:]
			for i in range(1,len(cols),1):
				w=cols[i].strip()
				if len(w) == 0:
					continue
				if w.startswith("###"):
					ws=cs[w]
				else:
					ws={w:1}
				for w in ws:
					if w not in ers:
						ers[w]={w:1}
					ers[w][head]=1
		else:
			for i in range(1,len(cols),1):
				w=cols[i].strip()
				if len(w) == 0:
					continue
				if  w.startswith("###"):
					ws=cs[w]
				else:
					ws={w:1}
				if head not in ers:
					ers[head]={head:1}
				for w in ws:
					ers[head][w]=1
	for head in ers:
		sys.stderr.write(head+"\n")
		for w in ers[head]:
			sys.stderr.write("\t"+w+"\n")
	return ers

searchHead={
"recipe.getcollectrecomment":1,
"search.gethotsearch":1,
"search.getsuggestion":1,
"search.getcatelist":1,
"search.getlist":1,
"info.getinfo":1,
"recipephoto.getproducts":1,
"recipe.getcollectlist":1
}

def searchFollow(ers):
	for m in searchHead:
		if m not in ers:
			ers[m]={}
	lastIp=""
	lastWds={}
	lastTids={}
	liveMethods={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		ip=cols[0]
		if ip == "":
			continue
		u=column.ActionUser(cols[1:])
		if u != None and u != "":
			ip+="_"+u
		if lastIp == "":
			lastIp=ip
		if lastIp != ip:
			lastWds={}
			lastTids={}
			liveQids={}
			liveMethods={}
			lastIp=ip
		method=cols[column.METHOD_CID+1]
		v=column.bigVersion(cols[column.VERSION_CID+1])
		if method.startswith("search.getlist"):
			para=cols[column.PARA_ID+1]
			offset=column.getValue(para,"offset")
			if offset != "0":
				#sys.stderr.write("offset:"+offset+"\n")
				continue
			tid=column.getValue(para,"tagid")
			if tid != None and tid != "" and tid != "null":
				r=column.getValue(para,"return_request_id")
				if r != None and r != "":
					print "recipe.getcollectlist_return_"+method+"_"+v+"\t"+ip+"\ttid_"+tid
					continue
				if tid in lastTids and len(lastTids[tid]) >= 1:
					print lastTids[tid][-1]+"_"+method+"_"+v+"\t"+ip+"\ttid_"+tid
				else:
					print "VoidMethod_tid_"+method+"_"+v+"\t"+ip+"\ttid_"+tid
			else:
				kw=column.getValue(para,"keyword")
				if kw in lastWds and len(lastWds[kw]) >=1:
					print lastWds[kw][-1]+"_"+method+"_"+v+"\t"+ip+"\tkw_"+kw
				else:
					print "VoidMethod_kw_"+method+"_"+v+"\t"+ip+"\tkw_"+kw
		elif method in searchHead:
			tmp={}
			for	m in liveMethods:
				if method in ers[m]:
					tmp[m]=1
			for m in tmp:
				for tid in lastTids:
					ms=lastTids[tid]
					for i in range(len(ms)-1,-1,-1):
						if ms[i] == m:
							del ms[i]
				for kw in lastWds:
					ms=lastWds[kw]
					for i in range(len(ms)-1,-1,-1):
						if ms[i] == m:
							del ms[i]
				#sys.stderr.write("delete "+m+" because "+method+"\n")
				del liveMethods[m]
			liveMethods[method]=1
			kwret=None
			tidret=None
			if method == "search.gethotsearch" or method == "search.getsuggestion":
				ret=column2.FuncMap[method](cols[-1])
				if ret != None:
					kwret=[]
					for w in ret:
						kwret.append(column2.escapeUnicode(w).encode("utf-8"))
			elif method == "search.getcatelist":
				tidret=column2.FuncMap[method](cols[-1])
			elif method == "recipe.getcollectlist":
				ret=column2.FuncMap[method](cols[-1])
				if ret != None and "tag" in ret:
					tidret=[]
					for id,title in ret["tag"]:
						tidret.append(id)
			elif method == "info.getinfo":
				ret=column2.FuncMap[method](cols[-1])
				if ret != None and "tags" in ret:
					tidret=ret["tags"]
			if kwret != None:
				for kw in kwret:
					if kw not in lastWds:
						lastWds[kw]=[]
					lastWds[kw].append(method)
			if tidret != None:
				for tid in tidret:
					if tid not in lastTids:
						lastTids[tid]=[]
					lastTids[tid].append(method)
infoHead={
"rank.getrankview":"id",
"suggest.top":1,
"recipephoto.getproducts":1,
"search.getlist":"return_request_id:bool  tagid:bool      keyword:bool",
"search.getlistv3":1,
"recipe.getcollectrecomment":"type    offset",
"info.getalbuminfo":1,
"userfeed.getfollowuserfeed":1,
"recipeuser.getuserrecipelist":1
}

def info():
	lastU=""
	lastM="_"
	lastPara="_"
	lastQid="_"
	infoList=[]
	infoM={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < column.APP_LOG_COLUMNS:
			continue
		u=column.uuidFirst(cols[1:])
		v=column.bigVersion(cols[column.VERSION_CID+1])
		if v != "4":
			continue
		para=cols[column.PARA_ID+1]
		qid=column.getValue(para,"request_id")
		if qid == "" or u == None or u == "":
			if u == None:
				u=""
			type="_"
			if qid != "":
				type+="q"
			if u != "":
				type+="u"
			print "WW\t"+type+"\t"+u+"\t"+qid
			continue
		if lastU == "":
			lastU=u
		if lastU != u:
			lastU=u
			lastPara="_"
			lastQid="_"
			lastM="_"
			infoList=[]
			infoM={}
		method=cols[column.METHOD_CID+1]
		if method.startswith("info.getinfo") or method == "info.getlastestinfo":
			rid=column.getValue(para,"rid")
			if rid != None and rid != "":
				if rid in infoM:
					print infoM[rid][0]+"\t"+infoM[rid][1]+"\t"+u+"\t"+infoM[rid][2]+"\t"+rid
				else:
					print "VoidMethod_"+lastM+"\t"+lastPara+"\t"+u+"\t"+lastQid+"\t"+rid
		if method in infoHead:
			lastM=method
			infoList=[]
			lastPara="_"
			lastQid="_"
			ret=column2.FuncMap[method](cols[-1])
			if ret == None:
				sys.stderr.write(line)
				continue
			if method == "rank.getrankview":
				infoList=ret["rids"]
				lastPara=column.getValue(para,"id")
			elif method == "info.getalbuminfo":
				infoList=ret["rids"]
			elif method == "recipeuser.getuserrecipelist":
				infoList=ret
			elif method == "userfeed.getfollowuserfeed":
				for (id,type) in ret:
					if type == "10":
						infoList.append(id)
			elif method.startswith("search.getlist"):
				infoList=ret["rids"]
				rqid=column.getValue(para,"return_request_id")
				tagid=column.getValue(para,"tagid")
				keyword=column.getValue(para,"keyword")
				offset=column.getValue(para,"offset")
				if rqid != None and rqid != "":
					lastPara+="r"
				if tagid != None and tagid != "":
					lastPara+="t"
				if keyword != None and keyword != "":
					lastPara+="k"
				if offset == "0":
					lastPara+="0"
			elif method == "recipephoto.getproducts":
				for pid,rid in ret["prids"]:
					if rid != "0":
						infoList.append(rid)
			elif method == "recipe.getcollectrecomment":
				for rid,isLike in ret:
					infoList.append(rid)
				type=column.getValue(para,"type")
				offset=column.getValue(para,"offset")
				lastPara+=type+"_"+offset
			print method+"\t"+lastPara+"\t"+u+"\t"+qid
			print "VoidMethod_"+method+"\t"+lastPara+"\t"+u+"\t"+qid
			lastQid=qid
			for rid in infoList:
				infoM[rid]=(method,lastPara,qid)


sys.path.append("../mlog/")
import mlog

def wiki():
	lastIp=""
	v=""
	selfIds={}
	lastIp=""
	allIds={}
	ids={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		ip=cols[0]
		if lastIp == "":
			lastIp=ip
		if lastIp != ip:
			lastIp=ip
			selfIds={}
			v=""
		if len(cols) == 3:
			topic=mlog.getTopic(cols[2])
			if topic == None:
				continue
			print "all_"+v+"\t"+cols[0]+"\t"+topic
			if topic in allIds:
				print allIds[topic]+"_"+v+"\t"+cols[0]+"\t"+topic
			if topic in selfIds:
				print selfIds[topic]+"_"+v+"\t"+cols[0]+"\t"+topic
			if topic in ids:
				print ids[topic]+"_"+v+"\t"+cols[0]+"\t"+topic
		else:
			if len(cols) < column.APP_LOG_COLUMNS+1:
				continue
			method=cols[column.METHOD_CID+1]
			v=cols[column.VERSION_CID+1]
			if method == "wiki.getlistbytype":
				if len(cols) < column.APP_LOG_COLUMNS4+2:
					continue
				ret=column2.wikiList(cols[-1])
				if ret == None:
					continue
				type=column.getValue(cols[column.PARA_ID+1],"type")
				if type == None or type =="":
					continue
				if type == "1":
					for tid in ret:
						ids[tid]="wikis"
				elif type == "2":
					for tid in ret:
						ids[tid]="tables"
			elif method == "recipe.getcollectlist":
				if len(cols) == column.APP_LOG_COLUMNS4+2:
					ret=column2.getRecoms(cols[-1])
					if ret == None:
						continue
					if "wiki" in ret:
						for id,title in ret["wiki"]:
							allIds[id]="wiki"
							ids[id]="wikis"
							selfIds[id]="self_wiki"
					if "table" in ret:
						for id,title in ret["table"]:
							allIds[id]="table"
							ids[id]="tables"
							selfIds[id]="self_table"

def searchFollow():
	ers=readErase()
	searchFollow(ers)


if __name__=="__main__":
	#searchFollow()
	#wiki()
	info()


