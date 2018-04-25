#encoding=utf-8

import sys
import operator

sys.path.append("./")
sys.path.append("../")
sys.path.append("../abtest")

import column
import column2

import actionInfo


import readTagWap
tagWaps=readTagWap.read()
nutriTags=readTagWap.readNutiWap()

#favorite.addrecipe albumid recipeid
#info.downloadinfo	rid
#comment.getlist rid offset type=0  要注意去重
#comment.addcomment rid content	atuid(@) rrid（@）type=0
#share.sharafeed	itemid	type=0
#like.add	id
#recipephoto.getproducts	id type=2
#recipephoto.photoview	id
#
#
#favorte.add	rid	type=2
#comment.getlist	rid type=1 offset  专辑
#comment.addcomment rid content atuid(@) rrid（@）type=1
#share.sharefeed    itemid  type=1
#recipe.getablumlist
#

#
#favorite.add rid type=3
#comment.getlist rid offset type=3
#share.sharafeed    itemid  type=3
#有的话题可以点击菜谱
#
#

seqMethods={
	"search.getlist":1,
	"search.getlistv3":1,
	"search.getsearchindex":1,
	"search.getcatelist":1,
	"search.gethotsearch":1,
	"search.getsuggestion":1,

	"search.getcatelist":1,
	"search.getcatelistv2":1,
	"search.getcatelistv3":1,

	"info.getinfo":1,
	"info.getinfov3":1,
	"info.getinfov2":1,
	"info.getlastestinfo":1,

	"topic.getgroupindexdata":1,         #list[TopicId] cate_infos[CateId]
	"topic.getlist":1,                  #cate_id      Title
	"topic.getinfo":1,
	
	#
	"recipe.getfindrecipe":1,          #"ItemType":"act" topic-(*).html learn-RecipeId-OpenUrl
	"recipephoto.getproducts":1,       #request中的id是response中的TopicId   Pid（列表第一个） TopicId
	"recipephoto.photoview":1,
	

	"info.getalbuminfo":1,
	"recipe.getalbumlist":1,
	"search.getalbumlist":1,

	"comment.getlist":1,
	"comment.addcomment":1,
	"share.sharefeed":1,
	"recipe.getablumlist":1,
	"like.add":1,
	"favorite.add":1,
	"info.downloadinfo":1,
	"favorite.addrecipe":1,

	"recipe.getcollectrecomment":1,
	"rank.getrankview":1,
	"rank.getranklist":1,
	"wiki.getlistbytype":1,
	"suggest.top":1,

	"favorite.isfav":1,

}

StopMethods={
	"common.adddevietoken":1,
	"mobiledevice.initandroiddevice":1,
	"appscreen.getimage":1,
	"ad.getallad":1,
	"update.forceupdate":1,
	"update.version":1,
	"ad.getadinmobi":1,
	"notice.getcount":1,
	"common.gettime":1,
	"favorite.isfav":1,  #这个方法经常不是主动调用
}

#搜索场景：分类t1 热门标签t2  列表标签k2 菜谱标签k3 关键词搜索k1
#

#iPhone       Android
#Version/4.0
#
def getActionUser(cols,lastAid):
	ip=cols[0]
	time=cols[1]
	user=actionInfo.UserInfo(ip)
	if cols[2].find("m.haodou.com") > 0 and cols[2].find("GET") > 0:
		cols=cols[2].strip().split("\01")
		if len(cols) < 5:
			return None,None
		if cols[2].find("iPhone") > 0:
			user.media="appstore"
		else:
			user.media="Android"
		p=cols[4].find("topic-")
		if p > 0:
			end=cols[4].find(".html",p)
			if end < 0:
				end=len(cols[4])
			id=cols[4][p+len("topic-"):end]
			p=cols[4].find("uuid=",end)
			if p > 0:
				end=cols[4].find("&",p+5)
				if end > 0:
					user.uuid=cols[4][p+5:end]
			action=actionInfo.Action(lastAid+1,time,"topic.getinfo",id,user)
			return (action,user)
		#标签词语的wap页
		p=cols[4].find("?tid=")
		if p >= 0:
			end=cols[4].find("&",p)
			if end < 0:
				end=len(cols[4])
			id=cols[4][p+len("?tid="):end]
			end=id.find("gid=")
			if end > 0:
				id=id[0:end]
			if cols[4].find("sub=two") >end or cols[4].find("sub=one") > end:
				method=cols[4]
				action=actionInfo.Action(lastAid+1,time,method,cols[4],user)
				return (action,user)
			action=actionInfo.Action(lastAid+1,time,"tag.wap",id,user)
			if id in tagWaps:
				action.response=tagWaps[id]
				action.para=tagWaps[id][0]
				return (action,user)
			else:
				sys.stderr.write(id+"\n")
				sys.stderr.write("\t".join(cols)+"\n")
		p=cols[4].find("/app/recipe/act/tag/foodlist.php")
		if p >= 0:
			action=actionInfo.Action(lastAid+1,time,"nutri.wap",cols[4],user)
			action.response=nutriTags
			return (action,user)
	else:
		if len(cols) < column.APP_LOG_COLUMNS+1:
			return None,None
		version=cols[column.VERSION_CID+1]
		v=column.intVersion(version)
		u=column.uuidFirst(cols[1:]) #获得uuid
		if u != None and len(u) > 60:
			u=None
		user.version=v
		user.uuid=u
		user.media=cols[column.DEVICE_CID+1]
		method=cols[column.METHOD_CID+1]
		if method in StopMethods:
			return None,None
		para=cols[column.PARA_ID+1]
		action=actionInfo.Action(lastAid+1,time,method,para,user)
		action.response=column2.getResp(method,cols[-1])
		return (action,user)
	return None,None


def addAction(action,itemActions):
	for item in action.resultItems():
		if item not in itemActions:
			itemActions[item]=[]
		itemActions[item].append(action)
	
def proline(cols,userActions):
	(action,user)=getActionUser(cols,-1)
	if action == None:
		return
	has=-1
	#print "len",len(userActions)
	#print "user",user.name()
	#与同ip的最多10个用户比较，避免同ip用户太多，比较过于耗时
	for i in range(50):
		k=-(i+1)
		if i >= len(userActions):
			break
		#print "\tother",userActions[k][0].name()
		#如果同ip用户可以合并，则合并
		if userActions[k][0].sameUserGivenIP(user) >= 0: # 在ip相同时，这个函数的结果>=0时为不冲突，可以视为同一个用户
			#print "\teq",userActions[k][0].name()
			userActions[k][0].merge(user)
			has=k+len(userActions)  #has对应已有用户的下标,+len是为了使得下标为正
			if k != -1:
				tmp=userActions[k] #被merge的用户应该向后移到列表末尾，因为其时间已经最新了。
				userActions[k]=userActions[-1]
				userActions[-1]=tmp
			break
	#如果不能合并，则创建新用户
	if has < 0:
		userActions.append((user,[],{}))
	has=-1 #has对应新用户的下标
	action.id=len(userActions[has][1])
	userActions[has][1].append(action)
	action.user=userActions[has][0]
	itemActions=userActions[has][2]
	#print "resultItems,",action.resultItems()
	item=action.getItem()
	#print "item/itemActions/IsIn",item,itemActions,(item in itemActions)
	#sys.stderr.write(str(item)+"\n")
	if item in itemActions:
		for i in range(len(itemActions[item])-1,-1,-1):
			a=itemActions[item][i]
			#print "a",a
			#print "action",action
			ok=a.addHit(action)
			if ok:
				break
	addAction(action,itemActions)

def output(userActions):
	for u,actions,itemActions in userActions:
		print u
		for action in actions:
			print action
			pass

def seqMapper(f):
	lastIP=""
	userActions=[]
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			continue
		ip=cols[0]
		if lastIP == "":
			lastIP=ip
		if lastIP != ip:
			output(userActions)
			userActions=[]
			lastIP=ip
		lastAid=proline(cols,userActions)
	output(userActions)

if __name__=="__main__":
	seqMapper(sys.stdin)


