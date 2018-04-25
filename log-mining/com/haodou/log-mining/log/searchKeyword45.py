#encoding=utf-8
#
#专门为4.5版本定制

import sys
import operator

sys.path.append("./")
sys.path.append("../")
sys.path.append("../abtest")

import column
import column2

from clickCount import *
from cardClick import *

CardFix="##Card##_"

def getRidPos(ret):
	rids={}
	if "rids" in ret:
		size=FirstPageNum
		if len(ret["rids"]) < FirstPageNum:
			size=len(ret["rids"])
		for i in range(size):
				rids[ret["rids"][i]]=i
	return rids

THead=".html?id="
THeadLen=len(THead)

inn=0

def searchFollowMapper(f):
	lastIP=""
	rids={}
	oc=0
	otherRids={}
	otherAids={}
	otherTids={}
	searchRet={}
	click=None
	keywordClick={}
	lastK=""
	hc=0
	keyword=""
	cardBank=CardBank()
	for line in f:
		if True:
			cols=line.strip().split("\t")
			if len(cols) < 3:
				continue
			ip=cols[0]
			if lastIP == "":
				lastIP=ip
			if lastIP != ip:
				lastIP=ip
				keyword=""
				rids={}
				uc={}
				otherRids={}
				otherAids={}
				otherTids={}
				searchRet={}
				click=None
				lastSearchRet={}
				lastClick=None
				cardBank.reset()
			if line.find("m.haodou.com") > 0 and line.find("GET") > 0:
				cols=cols[2].strip().split("\01")
				if len(cols) < 5:
					continue
				p=cols[4].find(THead)
				if p > 0:
					end=cols[4].find("&",p+THeadLen)
					if end < 0:
						end=len(cols[4])
					id=cols[4][p+THeadLen:end]
					if id !="":
						if "topicId" in searchRet and searchRet["topicId"] == id:
							click.addTopicHit(id)
							oc+=1
							if "ttitle" in searchRet:
								title=searchRet["ttitle"]
								click.addTopicTitleHit(title,keyword)
						elif id in otherTids:
							oc+=1
							click.getMs().tb+=1
						else:
							if cols[4].find("uuid") > 0:
								sys.stderr.write(line)
						mtid=id
				continue
			if len(cols) < column.APP_LOG_COLUMNS+1:
				continue
			version=cols[column.VERSION_CID+1]
			v=column.intVersion(version)
			if v < 400:
				continue
			u=column.uuidFirst(cols[1:]) #获得uuid
			if u == None or u.find("{") >= 0:
				u=""
			para=cols[column.PARA_ID+1] #得到请求参数
			
			method=cols[column.METHOD_CID+1]  #获得请求的方法
			hasSearch=False
			if method == 'search.getsearchindex': #搜索方法
				keyword=V45Fix+column.getValue(para,"keyword") #搜索的关键字
				hasSearch=True
				searchRet=column2.FuncMap[method](cols[-1])   #获得搜索返回的食谱列表
				rids=getRidPos(searchRet)
				if keyword not in keywordClick:
					keywordClick[keyword]=Click()
				click=keywordClick[keyword]
				click.addSearchRet(searchRet,keyword)
				cardBank.addSearch(keyword[len(V45Fix):],searchRet)
				
				otherRids={}
				otherAids={}
				otherTids={}
				if "rids" in searchRet:
					for i in range(FirstPageNum,len(searchRet["rids"]),1):
						otherRids[searchRet["rids"][i]]=i
			elif method == "search.getlist" and (v < 450 or v >= 480):
				offset=column.getValue(para,"offset")
				scene=column.getValue(para,"scene")	
				if scene != "k1":
					continue
				#keyword=column.getValue(para,"keyword") #搜索的关键字
				searchRet=column2.FuncMap[method](cols[-1]) #
				if offset != "0":
					kw=column.getValue(para,"keyword")
					if kw != keyword:
						continue
					if "rids" in searchRet:
						for i in range(len(searchRet["rids"])):
							otherRids[searchRet["rids"][i]]=i
					continue
				keyword=column.getValue(para,"keyword")
				hasSearch=True
				rids=getRidPos(searchRet)
				otherRids={}
				#otherAids={}
				#otherTids={}
				if "rids" in searchRet:
					for i in range(len(searchRet["rids"])):
						if i >= FirstPageNum:
							otherRids[searchRet["rids"][i]]=i
							continue
						rids[searchRet["rids"][i]]=i
				if keyword not in keywordClick:
					keywordClick[keyword]=Click()
				click=keywordClick[keyword]
				click.addSearchRet(searchRet,keyword)
				cardBank.addSearch(keyword,searchRet,False)
			if hasSearch:
				if lastK != "":
					keywordClick[lastK].addHitCount(hc)
					if hc + oc > 0:
						keywordClick[lastK].addHasHit()
				hc=0
				oc=0
				lastK=keyword
			if click == None:
				continue
			if method == "info.getinfo":
				rid=column.getValue(para,"rid") #获得点击的食谱id
				pos=-1
				if rid in rids:
					click.addRecipeHit(rid,rids[rid])
					kw=keyword
					if v >= 450:
						kw=keyword[len(V45Fix):]
					cardBank.addHit(kw,rid,rids[rid],(v >= 450))
					hc+=1
					pos=rids[rid]
				elif rid in otherRids:
					oc+=1
					click.getMs().rb+=1
				if pos >= 0:
					ret=column2.FuncMap[method](cols[-1])
					if ret == None:
						pass
						#sys.stderr.write(cols[-1])
					elif "title" in ret:
						title=ret["title"]
						click.addTitleHit(title,pos,keyword)
			elif method == "info.getfoodinfo":
				fid=column.getValue(para,"foodid")
				if fid != "" and "food" in searchRet and fid == searchRet["food"]:
					click.addFoodHit(fid)
					oc+=1

			elif v >= 450:
				if method == "info.getalbuminfo":
					id=column.getValue(para,"aid") #获取点击的专辑id
					if id !="":
						if "aid" in searchRet and searchRet["aid"]==id :
							click.addAlbumHit(id)
							oc+=1
							if "atitle" in searchRet:
								title=searchRet["atitle"]
								click.addAlbumTitleHit(title,keyword)
						elif id in otherAids:
							oc+=1
							click.getMs().ab+=1
				elif method == "search.gettags":
					kw=V45Fix+column.getValue(para,"keyword") #搜索的关键字
					if kw != keyword:
						continue
					tags=column2.FuncMap[method](cols[-1])
					for tagid in tags:
						if tagid not in  click.getMs().rTagShow:
							click.getMs().rTagShow[tagid]=1
						else:
							click.getMs().rTagShow[tagid]+=1
				elif method == "search.getlist":
					offset=column.getValue(para,"offset")
					scene=column.getValue(para,"scene")
					if scene != "k1":
						continue
					kw=V45Fix+column.getValue(para,"keyword") #搜索的关键字
					if kw != keyword:
						continue
					searchRet=column2.FuncMap[method](cols[-1]) #
					if "rids" in searchRet:
						for i in range(len(searchRet["rids"])):
							otherRids[searchRet["rids"][i]]=i
					if offset != "0":
						continue
					tagid=column.getValue(para,"tagid")
					if tagid == "" or tagid == "null":
						click.getMs().r+=1
					else:
						if tagid not in click.getMs().rt:
							click.getMs().rt[tagid]=1
						else:
							click.getMs().rt[tagid]+=1
						click.getMs().rtn=1
				elif method == "search.getalbumlist":
					offset=column.getValue(para,"offset")
					kw=V45Fix+column.getValue(para,"keyword") #搜索的关键字
					if kw != keyword:
						continue
					ret=column2.getList(cols[-1],"AlbumId")
					for aid in ret:
						otherAids[aid]=1
					if offset == "0":
						click.getMs().a+=1
				elif method == "search.gettopiclist":
					offset=column.getValue(para,"offset")
					kw=V45Fix+column.getValue(para,"keyword") #搜索的关键字
					if kw != keyword:
						continue
					ret=column2.getList(cols[-1],"TopicId","int")
					for tid in ret:
						#sys.stderr.write("tid:"+tid+"\n")
						otherTids[tid]=1
					if offset == "0":
						click.getMs().t+=1
	if lastK != "":
		keywordClick[lastK].addHitCount(hc)
		if hc + oc > 0:
			keywordClick[lastK].addHasHit()
	ck45=Click()
	ck44=Click()
	for kw in keywordClick:
		if kw.startswith(V45Fix):
			ck45.merge(keywordClick[kw])
		else:
			ck44.merge(keywordClick[kw])
		print kw+"\t"+str(keywordClick[kw])
	print "ck45_##total##"+"\t"+str(ck45)
	print "ck44_##total##"+"\t"+str(ck44)
	for card in cardBank.bank:
		print CardFix+str(cardBank.bank[card])

def searchFollowReducer(f):
	lastK=""
	ck=Click()
	for line in sys.stdin:
		cols=line.split("\t")
		kw=cols[0]
		#空搜索
		if kw.strip() == "":
			kw="[VOID]"
		if lastK == "":
			lastK=kw
			if kw.startswith(CardFix):
				cc=CardClick(kw)
		if lastK != kw:
			if lastK.startswith(CardFix):
				print cc
			else:
				print lastK+"\t"+str(ck)
			if kw.startswith(CardFix):
				cc=CardClick(kw)
			else:
				ck=Click()
			lastK=kw
		try:		
			if line.startswith(CardFix):
				tcc=readCardClick(cols)
				cc.merge(tcc)
			else:
				(kw,tck)=readClick(cols)
				ck.merge(tck)
		except:
			sys.stderr.write(line)
	if lastK != "":
		if lastK.startswith(CardFix):
			print cc
		else:
			print lastK+"\t"+str(ck)

if __name__=="__main__":
	if sys.argv[1] == "map":
		searchFollowMapper(sys.stdin)
	elif sys.argv[1] == "reduce":
		searchFollowReducer(sys.stdin)
	

