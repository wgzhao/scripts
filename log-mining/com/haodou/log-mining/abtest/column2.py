#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../")
sys.path.append("../util")

import columnUtil

def escapeUnicode(v):
	if v.find("\u") >= 0:
		while v.endswith("\\"):
			v=v[0:-1]
		v=eval('u'+'"'+v+'"')
	return v

def escapeUtf8(v):
	return escapeUnicode(v).encode("utf-8")

def getBackValue(para,before,after,begin=0):
	pos=para.find(after,begin)
	if pos < 0:
		return (None,begin)
	pos1=para[0:pos].rfind(before)
	if pos1 > 0:
		v=para[pos1+len(before):pos]
		return (v,pos)
	return (None,pos)

def getValue(para,before,after,begin=0):
	pos=para.find(before,begin)
	#print para[begin:begin+100],before,pos
	if pos < 0:
		return (None,begin)
	pos1=para.find(after,pos+len(before))
	if pos1 > pos:
		v=para[pos+len(before):pos1]
		while v.endswith('}') or v.endswith(']') or v.endswith('"'):
			v=v[0:-1]
		if v.startswith('"'):
			v=v[1:]
		return (v,pos1)
	return (None,pos)

def getFinds(response):
	ret={}
	endPos=0
	zzz=[]
	for i in range(3):
		(id,endPos)=getValue(response,'"TopicId":',',',endPos)
		(day,endPos)=getValue(response,'"ThemeTitle":"','"',endPos)
		if endPos <= 0 or day == None or len(day) < 27:
			sys.stderr.write("day is null for getFinds\n")
			return None
		else:
			day=escapeUtf8(day)
			zzz.append((id,day))
	ret["zzz"]=zzz
	(gg,endPos)=getValue(response,"Fview.php%3Fid%3D",'"',endPos)
	if gg == None:
		(gg,endPos)=getValue(response,'Fapp%2Frecipe%2Fact%2','"',endPos)
		if gg==None:
			#sys.stderr.write("gg is null for getFinds\n")
			return None
	ret["gg"]=gg
	(rid,endPos)=getValue(response,'"RecipeId":',',',endPos)
	if rid == None:
		sys.stderr.write("RecipeId is null for getFinds\n")
		return None
	else:
		ret["learnRecipeId"]=rid
	ret["topic"]=[]
	for i in range(2):
		(tname,endPos)=getValue(response,'"TopicName":"','"',endPos)
		if tname == None:
			sys.stderr.write("TopicName is null for getFinds\n")
			return None
		tids=[]
		for k in range(4):
			(tid,endPos)=getValue(response,'"Id":',',',endPos)
			if tid != None:
				tids.append(tid)
		tname=escapeUtf8(tname)
		ret["topic"].append((tname,tids))
	uids=[]
	for i in range(6):
		(uid,endPos)=getValue(response,'"UserId":',',',endPos)
		if uid != None:
			uids.append(uid)
	ret["user"]=uids
	return ret

def getRecoms(response):
	ret={}
	pos=response.find('"ad"')
	ret["ad"]=[]
	endPos=0
	while pos > 0:
		pos1=response.find('"ad"',pos+1)
		if pos1 > 0:
			(gg,endPos)=getValue(response,'"Url":"','"',pos)
			ret["ad"].append(gg)
		pos=pos1
	ret["sort"]=[]
	endPos+=10
	while endPos > 0:
		max=endPos
		(id,tPos)=getValue(response,'{"Id":',',',endPos)
		if id == None:
			#sys.stderr.write("id is null for getRecoms\n")
			break
		if tPos > max:
			max=tPos
		(title,tPos)=getValue(response,'"Title":"','"',endPos)
		if title == None:
			#sys.stderr.write("title is null for getRecoms\n")
			break
		if tPos > max:
			max=tPos
		title=escapeUtf8(title)
		(type,tPos)=getValue(response,'"Type":"','"',endPos)
		if type == None:
			#sys.stderr.write("type is null for getRecoms\n")
			break
		if tPos > max:
			max=tPos
		(sortIndex,tPos)=getValue(response,'"SortIndex":',',',endPos)
		if sortIndex == None:
			break
		if tPos > max:
			max=tPos
		endPos=max+10
		if type not in ret:
			ret[type]=[]
		ret["sort"].append((title,sortIndex))
		ret[type].append((id,title))
	return ret

def getSearch(response):
	ret={}
	endPos=1
	(fid,endPos)=getValue(response,'"Id":',',',endPos)
	if fid != None and fid != "0":
		ret["food"]=fid
	ret["rids"]=[]
	ret["rtitles"]=[]
	while endPos > 0:
		(rid,endPos)=getValue(response,'"RecipeId":',',',endPos)
		if rid == None:
			(rid,endPos)=getValue(response,'"Rid":',',',endPos)
		if rid == None:
			break
		ret["rids"].append(rid)
		(title,tpos)=getValue(response,'"Title":"','"',endPos)
		if title == None:
			title=""
		else:
			title=escapeUtf8(title)
		ret["rtitles"].append(title)
		(Card,cpos)=getValue(response,'"Card":"','"',endPos)
		if Card != None and Card != "":
			if "card" not in ret:
				ret["card"]={}
			ret["card"][rid]=escapeUtf8(Card)
	return ret

#
#旧版："Name": "场景","Sub": [{"Name": "日常","Tags": [{"Id": 286,"Name": "早餐"
#
#
def getCatelist(response):
	ret=[]
	(cate,endPos)=getValue(response,'"Cate":"','"',0)
	if cate == None:
		(subName,nextSubPos)=getBackValue(response,'"Name":"','","Sub":',0)
		while True:
			if subName == None:
				break
			#print "subName",subName,nextSubPos
			ret.append((escapeUtf8(subName),[]))
			tagsName=None
			tagPos=nextSubPos
			(subName,nextSubPos)=getBackValue(response,'"Name":"','","Sub":',nextSubPos+1)
			if subName == None:
				nextSubPos=len(response)
			#print "tagPos",tagPos,subName,nextSubPos
			while tagPos < nextSubPos:
				(tagsName,tagPos)=getBackValue(response,'"Name":"','","Tags":',tagPos+1)
				#print tagsName,tagPos
				if tagsName == None:
					break
				if tagPos < nextSubPos:
					ret[-1][1].append((escapeUtf8(tagsName),[]))
					tPos=tagPos
					(tagsName,nextTagPos)=getBackValue(response,'"Name":"','","Tags":',tagPos+1)
					if nextTagPos > nextSubPos or tagsName == None:
						nextTagPos=nextSubPos
					while tPos < nextTagPos:
						(tName,tPos)=getValue(response,'"Id":',',',tPos)
						if tName == None:
							break
						if tPos < nextTagPos:
							ret[-1][1][-1][1].append(tName)
						else:
							tPos=nextTagPos
				else:
					tagPos=nextSubPos
			if tagsName == None:
				break
	while endPos > 0:
		if cate == None:
			break
		ret.append((escapeUtf8(cate),[]))
		(cate,nextCatePos)=getValue(response,'"Cate":"','"',endPos)
		if cate == None:
			nextCatePos=len(response)
		while endPos < nextCatePos:
			(tid,endPos)=getValue(response,'"Id":',',',endPos)
			if tid == None:
				break
			if endPos < nextCatePos:
				ret[-1][1].append(tid)
			else:
				endPos=nextCatePos
		if tid == None:
			break
	return ret



def getRecomList(response):
	ret=[]
	endPos=1
	while endPos > 0:
		(rid,endPos)=getValue(response,'"RecipeId":',',',endPos)
		(isLike,endPos)=getValue(response,'"IsLike":',',',endPos)
		(tags,endPos)=getValue(response,'"Tags":',']',endPos)
		if rid == None or isLike == None:
			break
		if tags != None:
			ts=getList(tags,'Id','int')
		else:
			ts=[]
		ret.append((rid,isLike,ts))
	return ret


def getList(response,name,type="str"):
	ret=[]
	endPos=1
	while endPos > 0:
		if type == "str":
			(rid,endPos)=getValue(response,'"'+name+'":"','"',endPos)
		else:
			(rid,endPos)=getValue(response,'"'+name+'":',',',endPos)
		if rid == None:
			break
		ret.append(rid)
	return ret

def getNameList(response,listName,eleName,type):
	p=response.find(listName)
	if p > 0:
		return getList(response[p:],eleName,type)
	return None

def getInfo(response):
	ret={}
	endPos=1
	(rid,endPos)=getValue(response,'"RecipeId":',',',endPos)
	if rid == None:
		return ret
	(title,endPos)=getValue(response,'"Title":"','"',endPos)
	if title != None:
		title=escapeUtf8(title)
		ret["title"]=title
	if endPos < 0:
		endPos=0
	(tags,endPos)=getValue(response,'"Tags"','[',endPos)
	pos1=response.find(']')
	if pos1 < 0:
		return ret
	while endPos > 0 and endPos < pos1:
		(tid,endPos)=getValue(response,'"Id":',',',endPos)
		if tid == None:
			break
		if "tags" not in ret:
			ret["tags"]=[]
		ret["tags"].append(tid)
	while endPos >	0:
		(fid,endPos)=getValue(response,'"name":"','"',endPos)
		if fid == None:
			break
		if "stuffs" not in ret:
			ret["stuffs"]=[]
		ret["stuffs"].append(escapeUtf8(fid))
	while endPos > 0:
		(pid,endPos)=getValue(response,'"Pid":',',',endPos)
		if pid == None:
			break
		if "pids" not in ret:
			ret["pids"]=[]
		ret["pids"].append(pid)
	return ret	

def wikiList(response):
	ret=[]
	endPos=1
	while endPos > 0:
		(tid,endPos)=getValue(response,'"ItemId":',',',endPos)
		if tid == None:
			break
		ret.append(tid)
	return ret

def getSuggestList(response):
	(listStr,endPos)=getValue(response,'"list":["','"]',0)
	if listStr == None:
		return []
	list=listStr.split('","')
	ret=[]
	for t in list:
		ret.append(escapeUtf8(t))
	return ret

def getSuggestRecipe(response):
	ret={}
	p=response.find('"tag_list"')
	rids=getList(response[0:p],"Rid")
	if len(rids) == 0:
		rids=getList(response[0:p],"Rid","int")
	if len(rids) > 0:
		ret["rids"]=rids
	if p < 0:
		return ret
	tags=getList(response[p:],"Id","int")
	if len(tags) > 0:
		ret["tags"]=tags
	p=response.find('"collect_list"',p)
	if p < 0:
		return ret
	aids=getList(response[p:],"Cid","int")
	if len(aids) > 0:
		ret["aids"]=aids
	p=response.find('"wiki_list"',p)
	if p < 0:
		p=response.find('"ad_data"',p)
		ads=getList(response[p:],"Url")
		if len(ads) > 0:
			ret["ad"]=ads
		return ret
	wikis=[]
	while p > 0:
		s=response.find('?id=',p)
		if s< 0:
			break
		e=response.find('&',s)
		if e < 0:
			break
		wikis.append(response[s+4:e])
		p=e
	if len(wikis) >0:
		ret["wikis"]=wikis
	return ret

def getHomeIndex(response):
	ret={}
	rids=getList(response,"RecipeId","int")
	if len(rids) > 0:
		ret["rids"]=rids
	aids=getList(response,"AlbumId","int")
	if len(aids) > 0:
		ret["aids"]=aids
	p=response.find('"wiki_list"')
	wikis=[]
	urls=getList(response,"Url","int")
	for url in urls:
		wikis.append(columnUtil.parseUrl(url))
	if len(wikis) >0:
		ret["wikis"]=wikis
	return ret


def topicList(response):
	ret=[]
	endPos=1
	while endPos > 0:
		(tid,endPos)=getValue(response,'"TopicId":',',',endPos)
		if tid == None:
			break
		ret.append(tid)
	return ret

def getTitleList(response):
	ret={}
	endPos=0
	(title,endPos)=getValue(response,'"AlbumTitle":"','"',endPos)
	if title == None:
		(title,endPos)=getValue(response,'"Title":"','"',endPos)
	if title == None:
		sys.stderr.write("response:"+response+"\n")
		return {}
	ret["title"]=escapeUtf8(title)
	ret["rids"]=[]
	while endPos > 0:
		(rid,endPos)=getValue(response,'"RecipeId":',',',endPos)
		if rid == None:
			break
		ret["rids"].append(rid)
	return ret

def getTitlePRidList(response):
	ret={}
	endPos=0
	(title,endPos)=getValue(response,'"Title":"','"',endPos)
	if title == None:
		sys.stderr.write("response:"+response+"\n")
		return {}
	ret["Title"]=title
	ret["prids"]=[]
	while endPos > 0:
		(pid,endPos)=getValue(response,'"Pid":',',',endPos)
		(rid,endPos)=getValue(response,'"Rid":',',',endPos)
		if pid == None or rid == None:
			break
		ret["prids"].append((pid,rid))
	return ret

def getfollowuserfeed(response):
	ret=[]
	endPos=1
	while endPos > 0:
		(ItemId,endPos)=getValue(response,'"ItemId":',',',endPos)
		(type,endPos)=getValue(response,'"Type":',',',endPos)
		if ItemId == None or type == None:
			break
		ret.append((ItemId,type))
	return ret

def getlist(response,name,t):
	if t == "int":
		t=''
		e=','
	else:
		t='"'
		e='"'
	ret=[]
	endPos=1
	while endPos > 0:
		(v,endPos)=getValue(response,'"'+name+'":'+t,e,endPos)
		if v == None:
			break
		ret.append(v)
	return ret

def getuserrecipelist(response):
	return getlist(response,"Rid","int")
	
def getSearchIndex(response):
	ret={}
	endPos=1
	(fid,endPos)=getValue(response,'"Id":',',',endPos)
	if fid != None and fid != "0":
		ret["food"]=fid
	ret["rids"]=[]
	ret["rtitles"]=[]
	while True:
		(rid,endPos)=getValue(response,'"RecipeId":',',',endPos)
		if rid == None:
			(rid,endPos)=getValue(response,'"Rid":',',',endPos)
		if rid == None:
			break
		ret["rids"].append(rid)
		(title,tpos)=getValue(response,'"Title":"','"',endPos)
		if title == None:
			title=""
		else:
			title=escapeUtf8(title)
		ret["rtitles"].append(title)
		(Card,cardPos)=getValue(response,'"Card":"','"',endPos)
		if Card != None and Card != "":
			if "card" not in ret:
				ret["card"]={}
			ret["card"][rid]=escapeUtf8(Card)
	endPos=response.find("album")
	(aid,endPos)=getValue(response,'"AlbumId":"','"',endPos)
	if aid != None:
		ret["aid"]=aid
		(title,apos)=getValue(response,'"Title":"','"',endPos)
		if title == None:
			title=""
		else:
			title=escapeUtf8(title)
		ret["atitle"]=title
	(topicId,endPos)=getValue(response,'"TopicId":',',',endPos)
	if topicId != None:
		ret["topicId"]=topicId
		(title,apos)=getValue(response,'"Title":"','"',endPos)
		if title == None:
			title=""
		else:
			title=escapeUtf8(title)
		ret["ttitle"]=title
	return ret

def getWeekSugget(response):
	ret={}
	rids=getList(response,"RecipeId")
	if len(rids) == 0:
		rids=getList(response,"RecipeId","int")
	if len(rids) >= 0:
		ret["rids"]=rids
	p=response.find('"ad":')
	if p > 0:
		(ad,pos)=getValue(response,'"Url":"','"',p)
		if ad != None:
			ret["ad"]=ad
	return ret

def getSearchTopicList(response):
	return getList(response,"TopicId","int")


def getSearchAlbumList(response):
	return getList(response,"AlbumId","int")

def getUserIdList(response):
	return getList(response,"UserId","int")

def getRecipeAlbumList(response):
	ret=getList(response,"Id","int")
	return ret

def getRidList(response):
	return getList(response,"RecipeId","int")

def getSearchTags(response):
	ret=getList(response,"Id","int")
	return ret

def getErrorMessage(response):
	message=getValue(response,'"errormsg":"','"',0)[0]
	if message != None:
		message=escapeUtf8(message)
	return message

def getReg(response):
	UserId=getValue(response,'"UserId":"','"',0)[0]
	if UserId == None:
		message=getErrorMessage(response)
		if message==None:
			return "message_"
		else:
			return "message_"+message
	else:
		return UserId

def getBindRet(response):
	UserId=getValue(response,'"UserId":"','"',0)[0]
	if UserId == None:
		openid=getValue(response,'"openid":"','"',0)[0]
		if openid == None:
			openid=""
			message=getErrorMessage(response)
			if message==None:
				return "message_"
			else:
				return "message_"+message
		else:
			return "openid_"+openid
	else:
		return UserId

def getVideoIndex(response):
	fp=response.find('"focus_recommend"')
	lp=response.find('"index_recommend":')
	if fp < lp:
		fstr=response[fp:lp]
		lstr=response[lp:]
	else:
		fstr=response[fp:]
		lstr=response[lp:fp]
	(fid,pos)=getValue(fstr,'"RecipeId":"','"')
	llist=getList(lstr,'RecipeId')
	clist=getList(lstr,'CateId')
	return (fid,llist,clist)

def bindNew(response):
	UserId=getValue(response,'"UserId":"','"',0)[0]
	if UserId == None:
		message=getErrorMessage(response)
		if message==None:
			return "message_"
		else:
			return "message_"+message
	return UserId

def getVideoList(response):
	clist=getList(response,"CateId")
	rlist=getList(response,"RecipeId")
	return (clist,rlist)

def getIndexIndex(response):
	p=response.find('"recipe":{')
	if p < 0:
		return []
	e=response.find("]",p)
	if e < 0:
		return []
	ulist=getList(response[p:e],"Url")
	return ulist

FuncMap={"recipe.getcollectlist":getRecoms,
	"recipe.getfindrecipe":getFinds,
	"search.getlist":getSearchIndex,
	"search.getcatelist":getCatelist,
	"search.getcatelistv2":getCatelist,
	"search.getcatelistv3":getCatelist,
	"recipe.getcollectrecomment":getRecomList,
	"info.getinfo":getInfo,
	"search.getlistv3":getSearch,
	"search.getlistv2":getSearch,
	"info.getinfov3":getInfo,
	"info.getinfov2":getInfo,
	"info.getlastestinfo":getInfo,
	"wiki.getlistbytype":wikiList,
	"search.getsuggestion":getSuggestList,
	"search.gethotsearch":getSuggestList,
	"topic.getlist":topicList,
	"rank.getrankview":getTitleList,
	"info.getalbuminfo":getTitleList,
	"suggest.top":getTitleList,
	"recipephoto.getproducts":getTitlePRidList,
	"userfeed.getfollowuserfeed":getfollowuserfeed,
	"recipeuser.getuserrecipelist":getuserrecipelist,
	"recipeuser.getuserbyname":getUserIdList,
	"search.getsearchindex":getSearchIndex,
	"suggest.recipe":getSuggestRecipe,
	"suggest.recipev3":getSuggestRecipe,
	"suggest.recipead":getSuggestRecipe,
	"week.suggest":getWeekSugget,
	"search.gettopiclist":getSearchTopicList,
	"search.getalbumlist":getSearchAlbumList,
	"search.getmyfavlist":getRidList,
	"home.index":getHomeIndex,
	"home.indexv2":getHomeIndex,
	"recipe.getalbumlist":getRecipeAlbumList,
	"search.gettags":getSearchTags,
	"common.sendcode":getErrorMessage,
	"passport.reg":getReg,
	"passport.bindconnectstatus":getBindRet,
	"passport.loginbyconnect":bindNew,
	"info.getvideoindexdata":getVideoIndex,
	"info.getvideolist":getVideoList,
	"index.index":getIndexIndex,
}

def getResp(method,response):
	if method in FuncMap:
		return FuncMap[method](response)
	else:
		return {}

def testGetFinds():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 11:
			continue
		method=cols[9]
		if method != "recipe.getfindrecipe":
			continue
		ret=getFinds(cols[-1])
		if ret	!= None:
			pass
			#print "ret",ret
		else:
			print line

def testGetRecoms():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 11:
			continue
		method=cols[9]
		if method != "recipe.getcollectlist":
			continue
		if len(cols[-1]) < 200:
			continue
		print line
		ret=getRecoms(cols[-1])
		if ret != None:
			#pass
			print "ret",ret
		else:
			pass
			#print "line",line

def test2():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 12:
			continue
		method=cols[9]
		if method not in FuncMap:
			continue
		if len(cols[-1]) < 200:
			continue
		#ret=FuncMap[method](cols[-1])
		ret=getResp(method,cols[-1])
		if ret != None:
			print line
			print method+"\t"+str(ret)
			pass
		else:
			pass
			#sys.stderr.write(method+"\n")
			#sys.stderr.write(line)

#
#head -100 jqp0804.05 | python column2.py
if __name__=="__main__":
	#testGetFinds()
	#testGetRecoms()
	test2()


