#encoding=utf-8
import sys

sys.path.append("../")
import column

sys.path.append("../util")
import DBCateName

cates=DBCateName.readCateFile(open("cateidName.txt"))

def tagName(tagid):
	tagName=tagid
	if tagid != "" and tagid != "null":
		try:
			tagid=int(tagid)
			if tagid < len(cates) and cates[tagid] != "":
				tagName=cates[tagid]
		except:
			pass
	return tagName

def getSearchKey(para):
	scene=column.getValue(para,"scene")
	if scene.startswith("k"):
		kw="keyword-"+column.getValue(para,"keyword")
		if scene == "k1":
			tagid=column.getValue(para,"tagid")
			return kw+"-tag-"+tagName(tagid)
		else:
			return kw
	else:
		tagid=column.getValue(para,"tagid")
		if tagid == "":
			tagid=column.getValue(para,"typeid")
		if tagid != "" and tagid != "null":
			return "tag-"+tagName(tagid)
		elif scene.startswith("t"):
			return "tag-"
		return "keyword-"+column.getValue(para,"keyword")


def parseUrl(url):
	if url.find("topic") < 0:
		return url
	s=url.find("?id=")
	if s < 0:
		return url
	e=url.find("&")
	if e < 0:
		e=len(url)
	return "tid-"+url[s+len("?id="):e]

def getPhotoviewItem(method,para):
	return "pid-"+column.getValue(para,"id")

def getV4TypeId(id,type):
	if type == "0":
		return "rid-"+id
	elif type == "1":
		return "aid-"+id
	elif type == "2":
		return "pid-"+id
	elif type == "3":
		return "tid-"+id
	else:
		return "rid-"+id

def getShareItem(method,para):
	type=column.getValue(para,"type")
	itemid=column.getValue(para,"itemid")
	return getV4TypeId(itemid,type)

def getV3TypeId(id,type):
	if type == "1":
		return "rid-"+id
	elif type == "2":
		return "aid-"+id
	elif type == "3":
		return "tid-"+id
	else:
		return "rid-"+id

def getFavoriteItem(method,para):
	type=column.getValue(para,"type")
	rid=column.getValue(para,"rid")
	return getV3TypeId(rid,type) 

def getCommentItem(method,para):
	type=column.getValue(para,"type")
	rid=column.getValue(para,"rid")
	return getV4TypeId(rid,type)

def getTopicItem(method,para):
	return "tid-"+para

def getSearchItem(method,para):
	return getSearchKey(para)

def getRecomListItem(method,para):
	type=column.getValue(para,"type")
	return "recom-"+type

def getRankViewItem(method,para):
	id=column.getValue(para,"id")
	return "rank-id-"+id

def getFoodInfoItem(method,para):
	foodid=column.getValue(para,"foodid")
	return "food-id-"+foodid

def getLikeAddItem(method,para):
	return "rid-"+column.getValue(para,"id")

def getRidItem(method,para):
	return "rid-"+column.getValue(para,"rid")

def getAidItem(method,para):
	return "aid-"+column.getValue(para,"aid")

def getRecipeidItem(method,para):
	return "rid-"+column.getValue(para,"recipeid")

def getSearchSuggestItem(method,para):
	return "keyword-"+column.getValue(para,"keyword")

def getSearchTagsItem(method,para):
	return "keyword-"+column.getValue(para,"keyword")

def getUserByNameItem(method,para):
	return "name-"+column.getValue(para,"name")

def getFollowItem(method,para):
	return "uid-"+column.getValue(para,"fid")

def getVideoIndexItem(method,para):
	return "recom-视频菜谱"

def getTagWapItem(method,para):
	return "tag-"+para

def getVideoListItem(method,para):
	return "info.getvideolist-"+column.getValue(para,"cate_id")

itemFuncPair={
	"favorite.isfav":getFavoriteItem,
	"favorite.add":getFavoriteItem,
	"comment.getlist":getCommentItem,
	"comment.addcomment":getCommentItem,
	"search.getlist":getSearchItem,
	"search.getlistv3":getSearchItem,
	"search.getsearchindex":getSearchItem,
	"topic.getinfo":getTopicItem,
	"search.gettopiclist":getSearchItem,
	"search.getalbumlist":getSearchItem,
	"search.getmyfavlist":getSearchItem,
	"share.sharefeed":getShareItem,
	"recipe.getcollectrecomment":getRecomListItem,
	"recipephoto.photoview":getPhotoviewItem,
	"rank.getrankview":getRankViewItem,
	"info.getfoodinfo":getFoodInfoItem,
	"like.add":getLikeAddItem,
	"info.getinfo":getRidItem,
	"info.getinfov2":getRidItem,
	"info.getinfov3":getRidItem,
	"info.getlastestinfo":getRidItem,
	"info.getalbuminfo":getAidItem,
	"favorite.addrecipe":getRecipeidItem,
	"info.downloadinfo":getRidItem,
	"search.getsuggestion":getSearchSuggestItem,
	"search.gettags":getSearchTagsItem,
	"recipeuser.getuserbyname":getUserByNameItem,
	"recipeuser.follow":getFollowItem,
	"info.getvideoindexdata":getVideoIndexItem,
	"info.getvideolist":getVideoListItem,
	"tag.wap":getTagWapItem,
}

def getShareVersionItem(method,para,version):
	itemid=column.getValue(para,"itemid")
	type=column.getValue(para,"type")
	if version == None or version < 400:
		return getV3TypeId(itemid,type)
	else:
		return getV4TypeId(itemid,type)

itemVersionFuncPair={
	"share.share":getShareVersionItem,
}

def getActionItem(method,para,version):
	if method in itemFuncPair:
		return itemFuncPair[method](method,para)
	else:
		if method in itemVersionFuncPair:
			return itemVersionFuncPair[method](method,para,version)
		return method

def searchListItems(method,para,response):
	ret=[]
	if "food" in response:
		ret.append("food-id-"+response["food"])
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	if "aid" in response: #4.8版本以上有专辑
		ret.append("aid-"+response["aid"])
	return ret

def searchIndexItems(method,para,response):
	ret=[]
	if "food" in response:
		ret.append("food-id-"+response["food"])
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	key=getSearchKey(para)
	if len(key) > 0:
		ret.append(key)
	if "aid" in response:
		ret.append("aid-"+response["aid"])
	if "topicId" in response:
		ret.append("tid-"+response["topicId"])
	return ret

def albumItems(method,para,response):
	ret=[]
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	ret.append("aid-"+column.getValue(para,"aid"))
	return ret

def topicItems(method,para,response):
	ret=[]
	ret.append("tid-"+para)
	return ret

def cateListItems(method,para,response):
	ret=[]
	for (cate,clist) in response:
		for t in clist:
			if type(t) != tuple:
				ret.append("tag-"+tagName(t))
			else:
				tlist=t[1]
				for t1 in tlist:
					ret.append("tag-"+tagName(t1))
	return ret

def getInfoItems(method,para,response):
	ret=[]
	if "tags" in response:
		for tag in response["tags"]:
			ret.append("tag-"+tagName(tag))
	ret.append("rid-"+column.getValue(para,"rid"))
	if "pids" in response:
		for pid in response["pids"]:
			ret.append("pid-"+pid)
	return ret

recomPair={
	"album":"aid",
	"wiki":"tid",
	"table":"tid",
	"rank":"rank-id",
}

def getRecomItems(method,para,response):
	ret=[]
	for name in response:
		if name in recomPair:
			for id,title in response[name]:
				ret.append(recomPair[name]+"-"+id)
		elif name == "recipe":
			for id,title in response[name]:
				ret.append("recom-"+title)
		elif name == "tag":
			for id,title in response[name]:
				ret.append("tag-"+tagName(id))
	return ret

def getSuggetRecipeItems(method,para,response):
	ret=[]
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	if "tags" in response:
		for tag in response["tags"]:
			ret.append("tag-"+tagName(tag))
	if "ads" in response:
		for url in response["ads"]:
			ret.append(parseUrl(url))
	if "aids" in response:
		for aid in response["aids"]:
			ret.append("aid-"+aid)
	if "wikis" in response:
		for wiki in response["wikis"]:
			ret.append("tid-"+wiki)
	return ret

def getWeekSuggestItems(method,para,response):
	ret=[]
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	if "ad" in response:
		ad=response["ad"]
		ret.append(parseUrl(ad))
	return ret

def getRecomListItems(method,para,response):
	ret=[]
	for rid,isLike,ts in response:
		ret.append("rid-"+rid)
		for tag in ts:
			ret.append("tag-"+tagName(tag))
	return ret

def getRankViewItems(method,para,response):
	ret=[]
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	return ret

def getSearchAlbumListItems(method,para,response):
	ret=[]
	for aid in response:
		ret.append("aid-"+aid)
	return ret

def getSearchTopicListItems(method,para,response):
	ret=[]
	for tid in response:
		ret.append("tid-"+tid)
	return ret

def getHomeIndexItems(method,para,response):
	ret=[]
	if "rids" in response:
		for rid in response["rids"]:
			ret.append("rid-"+rid)
	if "aids" in response:
		for aid in response["aids"]:
			ret.append("aid-"+aid)
	if "wikis" in response:
		for tid in response["wikis"]:
			if len(tid) > 12:
				continue
			ret.append("tid-"+tid)
	return ret

def getSearchSuggestionItems(method,para,response):
	ret=[]
	for kw in response:
		#4.8版本新需求
		if kw.startswith('搜索"') and kw.endswith('"相关豆友'):
			kw=kw[len('搜索"'):-len('"相关豆友')]
			ret.append("name-"+kw)
		else:
			ret.append("keyword-"+kw)
	return ret

def getRecipeAlbumListItems(method,para,response):
	ret=[]
	for aid in response:
		ret.append("aid-"+aid)
	return ret

def getWikiListItems(method,para,response):
	ret=[]
	for tid in response:
		ret.append("tid-"+tid)
	return ret

def getSearchTagsItems(method,para,response):
	ret=[]
	keyword=column.getValue(para,"keyword")
	for id in response:
		ret.append("keyword-"+keyword+"-tag-"+tagName(id))
	return ret

def getUserIdListItems(method,para,response):
	ret=[]
	for uid in response:
		ret.append("uid-"+uid)
	return ret

def getRidListItems(method,para,response):
	ret=[]
	for rid in response:
		ret.append("rid-"+rid)
	return ret

def getVideoIndexItems(method,para,response):
	ret=[]
	(fid,llist,clist)=response
	if fid != None:
		ret.append("rid-"+fid)
	if llist != None:
		for rid in llist:
			ret.append("rid-"+rid)
	if clist != None:
		for cid in clist:
			ret.append("info.getvideolist-"+cid)
	return ret

def getTagWapItems(method,para,response):
	ret=[]
	(name,cards)=response
	for card in cards:
		for topic in cards[card]:
			for item in cards[card][topic]:
				ret.append(item)
	ret.append("keyword-"+name)
	return ret

def getNutriWapItems(method,para,response):
	ret=[]
	for t in response:
		for item in response[t]:
			ret.append(item)
	return ret

def getVideoListItems(method,para,response):
	ret=[]
	(clist,rlist)=response
	for cid in clist:
		ret.append("info.getvideolist-"+cid)
	for rid in rlist:
		ret.append("rid-"+rid)
	return ret

def getIndexIndexItems(method,para,response):
	ret=[]
	ulist=response
	for url in ulist:
		p=url.find("?id=")
		if p > 0:
			e=url.find("&",p)
			rid=url[p+len("?id="):e]
			ret.append("rid-"+rid)
	return ret

itemsPair={
	"search.getlist":searchListItems,
	"search.getlistv3":searchListItems,
	"search.getsearchindex":searchIndexItems,
	"search.getcatelistv3":cateListItems,
	"search.getcatelist":cateListItems,
	"search.getcatelistv2":cateListItems,
	"info.getalbuminfo":albumItems,
	"info.getinfo":getInfoItems,
	"info.getinfov2":getInfoItems,
	"info.getinfov3":getInfoItems,
	"recipe.getcollectlist":getRecomItems,
	"topic.getinfo":topicItems,
	"suggest.recipe":getSuggetRecipeItems,
	"suggest.recipev3":getSuggetRecipeItems,
	"suggest.recipead":getSuggetRecipeItems,
	"week.suggest":getWeekSuggestItems,
	"recipe.getcollectrecomment":getRecomListItems,
	"rank.getrankview":getRankViewItems,
	"search.gettopiclist":getSearchTopicListItems,
	"search.getalbumlist":getSearchAlbumListItems,
	"search.getmyfavlist":getRidListItems,
	"home.index":getHomeIndexItems,
	"home.indexv2":getHomeIndexItems,
	"search.getsuggestion":getSearchSuggestionItems,
	"recipe.getalbumlist":getRecipeAlbumListItems,
	"wiki.getlistbytype":getWikiListItems,
	"search.gettags":getSearchTagsItems,
	"recipeuser.getuserbyname":getUserIdListItems,
	"info.getvideoindexdata":getVideoIndexItems,
	"info.getvideolist":getVideoListItems,
	"tag.wap":getTagWapItems,
	"nutri.wap":getNutriWapItems,
	"index.index":getIndexIndexItems,
}

def actionResultItems(method,para,response):
	if method in itemsPair:
		ret=itemsPair[method](method,para,response)
		offset=column.getValue(para,"offset")
		if offset == "0":
			item=getActionItem(method,para,400)
			if item != "#":
				ret.append(item)
		return ret
	else:
		return []

