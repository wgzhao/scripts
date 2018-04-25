#encoding=utf-8

#	搜索页面点击率与无点击率的比例
#	搜索结果页平均翻页次数
#	搜索结果页菜谱平均点击个数
#	搜索联想页面，“搜索相关豆友” 的点击次数
#	该页面上专辑的点击次数，话题点击次数，以及他们各自的更多按钮点击次数。
#	我的收藏，搜索我收藏的菜谱使用次数。
#	我收藏的菜谱搜索结果，平均有多少个菜谱

import sys
sys.path.append("./")
sys.path.append("../")
import column
sys.path.append("../util")
import DictUtil
import hitItemName

from actionUserInfo import *
from actionInfo import *

def getHitRate(num,vs):
	num+=1e-32
	hitNum=0
	hitSum=0
	for n in vs:
		if n > 0:
			hitNum+=vs[n]
			hitSum+=vs[n]*n
	return (hitNum,hitNum/num,hitSum,hitSum/num)

def getCountStr(num,vs):
	num+=1e-32
	hitNum=0
	hitSum=0
	for n in vs:
		if n > 0:
			hitNum+=vs[n]
			hitSum+=vs[n]*n
	return "%d\t%.4f\t%d\t%.4f"%(hitNum,hitNum/num,hitSum,hitSum/num)

cs={}

class HitRet(object):
	def __init__(self):
		self.num=0 #点击次数     offset=0 或空
		self.pages={}  #翻页次数分布  pull中个数
		self.sons={}  #孩子点击次数分布    孩子数：话题+菜谱+专辑+更多菜谱、专辑、话题
		self.sonTypes={}  #孩子点击分布
		self.gsons={}
		self.gsonTypes={} #孙子点击分布
		self.wrongs={}
	
	def hitRate(self):
		return getHitRate(self.num,self.sons)

	def hitRateStr(self):
		return getCountStr(self.num,self.sons)

	def gsonRateStr(self):
		return getCountStr(self.num,self.gsons)

	def pageRateStr(self):
		return getCountStr(self.num,self.pages)

	def __str__(self):
		return "%d\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s"%(self.num,self.pages,self.sons,self.sonTypes,self.gsons,self.gsonTypes,self.wrongs,self.hitRateStr(),self.pageRateStr(),self.gsonRateStr())

	@staticmethod
	def read(cols):
		ret=HitRet()
		ret.num=int(cols[0])
		ret.pages=eval(cols[1])
		ret.sons=eval(cols[2])
		ret.sonTypes=eval(cols[3])
		ret.gsons=eval(cols[4])
		ret.gsonTypes=eval(cols[5])
		ret.wrongs=eval(cols[6])
		return ret

	def merge(self,other):
		self.num+=other.num
		DictUtil.merge(self.pages,other.pages)
		DictUtil.merge(self.sons,other.sons)
		DictUtil.merge2(self.sonTypes,other.sonTypes)
		DictUtil.merge(self.gsons,other.gsons)
		DictUtil.merge2(self.gsonTypes,other.gsonTypes)
		DictUtil.merge(self.wrongs,other.wrongs)

#要加上下拉页面的点击数目
def getSonTypes(id,actions):
	wrong=0
	ts={}
	for sid in actions[id].sons:
		if sid >= len(actions):
			wrong+=1
			continue
		DictUtil.addOne(ts,hitItemName.getName(actions[sid]))
	for pid in actions[id].pull:
		if pid >= len(actions):
			wrong+=1
			continue
		(pts,pwr)=getSonTypes(pid,actions)
		DictUtil.merge(ts,pts)
		wrong+=pwr
	return (ts,wrong)

def getGsonTypes(id,actions):
	wrong=0
	ts={}
	for sid in actions[id].sons:
		if sid >= len(actions):
			wrong+=1
			continue
		for gid in actions[sid].sons:
			if gid >= len(actions):
				wrong+=1
				continue
			DictUtil.addOne(ts,hitItemName.getName(actions[gid]))
	for pid in actions[id].pull:
		if pid >= len(actions):
			wrong+=1
			continue
		(pts,pwr)=getGsonTypes(pid,actions)
		DictUtil.merge(ts,pts)
		wrong+=pwr
	return (ts,wrong)

def addTs(sts,totalTypes,sums):
	ssum=0
	for t in sts:
		ssum+=sts[t]
		if t not in totalTypes:
			totalTypes[t]={}
		DictUtil.addOne(totalTypes[t],sts[t])
	DictUtil.addOne(sums,ssum)

def addAction(action,actions,ret):
	ret.num+=1
	DictUtil.addOne(ret.pages,len(action.pull))
	(sts,swr)=getSonTypes(action.id,actions)
	addTs(sts,ret.sonTypes,ret.sons)
	(gts,gwr)=getGsonTypes(action.id,actions)
	addTs(gts,ret.gsonTypes,ret.gsons)

def output(actions):
	if len(actions) == 0:
		return 0
	version=actions[0].user.version
	if version not in cs:
		cs[version]={}
	for action in actions:
		offset=column.getValue(action.para,"offset")
		if offset != "" and offset != "0":
			continue
		stype=hitItemName.getName(action)
		if stype not in cs[version]:
			cs[version][stype]=HitRet()
		ret=cs[version][stype]
		addAction(action,actions,ret)

def count():
	allCount=HitRet()
	actions=[]
	userinfo=None
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 2:
			sys.stderr.write(line)
			continue
		if cols[1] == "-1":
			output(actions)
			actions=[]
			userinfo=UserInfo.readUserInfo(cols)
			continue
		if userinfo == None:
			sys.stderr.write("userinfo is None:"+line)
			continue
		action=Action.readAction(cols,userinfo)
		if action == None:
			sys.stderr.write("action is None:"+line)
			continue
		if action.id > len(actions):
			sys.stderr.write("action.id > len(actions):id%d-len:%d:\t"%(action.id,len(actions))+line)
			DictUtil.addOne(allCount.wrongs,1)
			continue
		actions.append(action)
	output(actions)
	for version in cs:
		for stype in cs[version]:
			p=stype.find("_")
			if p < 0:
				p=len(stype)
			method=stype[0:p]
			print "%s\t%s\t%s"%(version,stype,cs[version][stype])

def readLine(line):
	cols=line.strip().split("\t")
	version=cols[0]
	stype=cols[1]
	sret=HitRet.read(cols[2:])
	return (version,stype,sret)

def merge():
	print "版本\t类型\t点击数\t翻页分布\t儿子点击分布\t儿子方法分布\t孙子点击分布\t孙子方法分布\t错误分布\t有儿子点击数\t有儿子点击率\t儿子点击数\t平均儿子点击数\t有翻页数\t有翻页率\t翻页数\t平均翻页数\t有孙子点击数\t有孙子点击率\t孙子点击数\t平均孙子点击数"
	cs={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		version=cols[0]
		if version not in cs:
			cs[version]={}
		stype=cols[1]
		if stype not in cs[version]:
			cs[version][stype]=HitRet()
		sret=HitRet.read(cols[2:])
		cs[version][stype].merge(sret)
	for version in cs:
		for stype in cs[version]:
			print "%s\t%s\t%s"%(version,stype,cs[version][stype])

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "merge":
			merge()
	else:
		count()


