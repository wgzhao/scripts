#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("./util/")
sys.path.append("../util/")
sys.path.append("../readDB")
sys.path.append("./readDB")


from DBUtil2 import *
import ReadDBConf

def personalTagStr(r):
	return "%d\t%s"%(r[0],r[1])

def readPersonalTag():
	queryPrintLarge("recipe_user","select userid,favorite from User where favorite != 'NULL'",N=500000,strFunc=personalTagStr)

def readUserInfo():
	queryPrintLarge("recipe_user","select userid,gender,favorite from User where Status=1",N=500000)

def readRecipeVideo():
	queryPrintLarge("haodou_recipe","select recipeid,status from Video",N=500000)

def readUserFollow():
	queryPrintLarge("haodou_center","select userid,FollowUserId from UserFollow",N=2000000)

def topicUserStr(r):
	return "tid-%d\t%d"%(r[0],r[1])

def readTopicUser():
	queryPrintLarge("haodou_center","select topicid,userid from GroupTopic where status = 1",N=2000000,strFunc=topicUserStr)

def topicContentStr(r):
	if r[1] == None:
		return "tid-%d"%(r[0])
	return "tid-%d\t%s"%(r[0]," ".join(r[1].encode("utf-8").split()))

def readTopicContent():
	#queryPrintLarge("haodou_center","select topicid,content from GroupTopic where status = 1 limit 10",N=2000,strFunc=topicUserStr)
	queryPrintLarge("haodou_center","select topicid,content from GroupTopic where status = 1",N=2000,strFunc=topicContentStr)

def readCateName():
	queryPrint("haodou_recipe","select catenameid,Name,userid from CateName")

def photoUserStr(r):
	return "pid-%d\t%d"%(r[0],r[1])

def readPhotoUser():
	queryPrintLarge("haodou_photo","select Id,UserId from Photo where status = 1",N=2000000,strFunc=photoUserStr)

def recipeUserStr(r):
	#if ReadDBConf.isVideo(r[0]):
	#	return "video-%d\t%d"%(r[0],r[1])
	#else:
	return "rid-%d\t%d"%(r[0],r[1])

def readRecipeUser():
	queryPrintLarge("haodou_recipe","select RecipeId,UserId from Recipe where status=0",N=2000000,strFunc=recipeUserStr)

def commonStrFunc(r):
	s=""
	for rc in r:
		if type(rc) == unicode:
			rcs=rc.encode("utf-8")
		elif type(rc) == long:
			rcs=str(rc)
			if rcs.endswith("L"):
				rcs=rcs[0:-1]
		else:
			rcs=str(rc)
		if len(s) > 0:
			s+="\t"
		s+=rcs
	return s

def readInvalidItem():
	queryPrintLarge("haodou_recipe","select RecipeId,UserId from Recipe where status!=0",N=2000000,strFunc=commonStrFunc,prefix="rid-")
	queryPrintLarge("recipe_user","select userid from User where Status!=1",N=500000,strFunc=commonStrFunc,prefix="")
	queryPrintLarge("haodou_center","select topicid,userid from GroupTopic where status != 1",N=2000000,strFunc=commonStrFunc,prefix="tid-")
	queryPrintLarge("haodou_photo","select Id,UserId from Photo where status != 1",N=2000000,strFunc=commonStrFunc,prefix="pid-")

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "userTag":
			readPersonalTag()
		elif sys.argv[1] == "isVideo":
			readRecipeVideo()
		elif sys.argv[1] == "userFollow":
			readUserFollow()
		elif sys.argv[1] == "userInfo":
			readUserInfo()
		elif sys.argv[1] == "topicUser":
			readTopicUser()
		elif sys.argv[1] == "topicContent":
			readTopicContent()
		elif sys.argv[1] == "photoUser":
			readPhotoUser()
		elif sys.argv[1] == "recipeUser":
			readRecipeUser()
		elif sys.argv[1] == "invalidItem":
			readInvalidItem()
		elif sys.argv[1] == "cateName":
			readCateName()


