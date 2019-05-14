#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../")
import column
import getActionItem

SplitSign="_"

#search.getlist* search.getsearchindex
def getSearchName(method,para):
	scene=column.getSearchType(method,para)
	if scene == "t2":
		tag=column.getValue(para,"tagid")
		if tag != "":
			scene+=SplitSign+getActionItem.tagName(tag)
	return scene

def getPara(method,para):
	return para

def paraName(para,name):
	return column.getValue(para,name)

NameParaFunc={
	"search.getlist":getSearchName,
	"search.getlistv2":getSearchName,
	"search.getlistv3":getSearchName,
	"search.getsearchindex":getSearchName,
	"tag.wap":getPara,
}

ParaFunc={
	"recipe.getcollectrecomment":"type",
	"rank.getrankview":"id",
	"topic.getlist":"cate_id",
	"wiki.getlistbytype":"type",
}

NameFunc={
	"info.getvideoindexdata":"视频菜谱",
	"nutri.wap":"营养宝典",
}

#
def getMethodName(method,para):
	if method in NameFunc:
		return NameFunc[method]
	elif method in ParaFunc:
		return method+SplitSign+paraName(para,ParaFunc[method])
	elif method in NameParaFunc:
		return method+SplitSign+NameParaFunc[method](method,para)
	else:
		return method
#
def getName(action):
	return getMethodName(action.method,action.para)


def getTypeName(name):
	p=name.find(SplitSign)
	if p > 0:
		name=name[0:p]
	return name

def packItem(name,item):
	if name.find(item) < 0:
		name+=SplitSign+item
	if name=="":
		name="@VOID@"
	return name

def getPosName(action):
	name=getName(action)
	item=action.getItem()
	return "".join(packItem(name,item).split())
	'''
	if name.find(item) < 0:
		name+="."+item
	if name == "":
		name="__VOID__"
	return name
	'''


