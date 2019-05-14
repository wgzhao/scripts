#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../")
from column import *
from userRecomConf import *

sys.path.append("../readDB")
import ReadDBConf


#info.getalbuminfo               aid
#collect.getinfo 旧版专辑
#
#info.getlastestinfo 旧版读取缓存的菜谱详情页
#info.getinfo
#
#recipeuser.getuserinfo    userid    获得用户信息
#search.getlist      keyword,typeid/tagid
#seach.getsearchindex
#favorite.add            rid  type=2 专辑  type=3 话题 type=9 加入话题小组，如“家常菜”
#Comment.addComment         type,rid
#Favorite.add(recipe)     rid  #收藏

#info.getfoodinfo foodid
#stuff.getstuffinfo  旧版食材百科
#info.getfoodinfo    食材百科

#info.downloadinfo   rid
#share.sharefeed/share.share   rid
#like.add    rid           #喜欢
#digg.digg   赞成果照
#recipephoto.upload   
#info.updateandpublic    发布菜谱
#
#recipephoto.upload      发布成果照      2
#

#topic.view 话题浏览  id  type=3
#topic.gettopiclistv5  cateid 获取话题列表v5
#
#ad.getRecommendAdList “我的”-“精品推荐”

def ridMethod(methodName,cols):
	rid=getRid(cols)
	if rid != "":
		return (methodName,"rid-"+rid)
	return None,None

def info(cols):
	return ridMethod("view",cols)

def like(cols):
	id=getValue(cols[PARA_ID],"id")
	if id != "":
		type=getValue(cols[PARA_ID],"type")
		if type == "recipe":
			return ("like","rid-"+id)
		else:
			return ("like","rid-"+id)
	return None,None

def download(cols):
	return ridMethod("download",cols)

def public(cols):
	return ridMethod("public",cols)

def albuminfo(cols):
	aid=getAid(cols)	
	if aid != "":
		return ("view","aid-"+aid)
	return None,None

def fav(cols):
	rid=getRid(cols)
	if rid != "":
		type=keyword=getValue(cols[PARA_ID],"type")
		if type == "0":
			return ("favorite","rid-"+rid)
		elif type == "2":
			return ("favorite","aid-"+rid)
		elif type == "3":
			return ("favorite","tid-"+rid)
	return None,None

def search(cols):
	keyword=getValue(cols[PARA_ID],"keyword")
	keyword="".join(keyword.split())
	if keyword != "":
		return ("keyword","word-"+keyword)
	typeid=getValue(cols[PARA_ID],"typeid")
	if typeid == "":
		typeid=getValue(cols[PARA_ID],"tagid")
	if typeid == "":
		typeid=getValue(cols[PARA_ID],"tagid")
	if ridP.match(typeid):
		return ("tag","tagid-"+typeid)
	return None,None

def likePhoto(cols):
	pid=getValue(cols[PARA_ID],"id")
	if pid != "":
		type=getValue(cols[PARA_ID],"type")
		if type == "1":
			return ("like","pid-"+pid)
		return ("like","pid-"+pid)
	return None,None

def publicPhoto(cols):
	pid=getPid(cols)
	if pid != "":
		return ("public","pid-"+pid)
	return None,None

def follow(cols):
	fid=getValue(cols[PARA_ID],"fid")
	if fid != "":
		return ("follow","uid-"+fid)
	return None,None

def topicView(cols): #版本5.0的话题浏览接口
	id=getValue(cols[PARA_ID],"id")
	id=checkTid(id)
	type=getValue(cols[PARA_ID],"type")
	if id != "" and type == "3":
		return ("view","tid-"+id)
	return None,None

methods={
	"info.getinfo":info,
	"info.getinfov2":info,
	"info.getinfov3":info,
	"info.getlastestinfo":info,
	"favorite.addrecipe":fav,
	"favorite.add":fav,
	"search.getlist":search,
	"search.getsearchindex":search,
	"search.getlistv2":search,
	"search.getlistv3":search,
	"info.downloadinfo":download,
	"like.add":like,
	"info.updateandpublic":public, #发布直接读数据库
	"info.getalbuminfo":albuminfo,
	"collect.getinfo":albuminfo,
	"digg.digg":likePhoto,
	"recipephoto.upload":publicPhoto,  #发布直接读数据库
	"recipeuser.follow":follow,
	"topic.view":topicView
}

def getAction(cols):
	u=uuid(cols)
	if u == None:
		return None,None,None
	if uid(cols) != "":
		u="uid-"+u
	method=cols[METHOD_CID]
	rid=getRid(cols)
	para=cols[PARA_ID]
	method=cols[METHOD_CID]
	if method in methods:
		action,entity=methods[method](cols)
		return u,action,entity
	return u,None,None

def test():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if isNutLine(cols):  #本地的解析结果，直接输出
			print line.strip()
			continue
		elif isAccLine(cols): #过去累加数据
			print line.strip()
			continue
		user,action,entity=getAction(cols)
		if action != None:
			time=cols[TIME_CID]
			nutPrint(user,action,entity,time)
			#print user+"\t"+UserAction+"\t"+action+"-"+item+"\t"+time
			#print action+"-"+item+"\t"+ActionUser+"\t"+user+"\t"+time
		else:
			sys.stderr.write(line)

if __name__=="__main__":
	test()

