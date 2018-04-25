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

from seqHit import *

class PosRet(object):
	def __init__(self):
		self.num=0 #栏目进入次数     offset=0 或空
		self.pages={}  #翻页次数分布  pull中个数
		self.sonNums={} #孩子数分布
		self.sons={}  #孩子点击分布 孩子展示次数，孩子点击次数，有孙子点击率

	def sonStr(self):
		s="{"
		nn=0
		for item in self.sons:
			(n,hn,gn)=self.sons[item]
			if hn <= 0:
				continue
			if nn > 0:
				s+=";"
			nn+=1
			s+="%s:[%d,%d,%d]"%(item,n,hn,gn)
		s+="}"
		return s

	def readSons(self,str):
		str=str[1:-1]
		tt=str.split(";")
		for t in tt:
			ss=t.split(":")
			if len(ss) != 2:
				#sys.stderr.write(t+"\n")
				continue
			item=ss[0]
			vs=ss[1]
			cc=vs[1:-1].split(",")
			if len(cc) != 3:
				sys.stderr.write(str+"\n")
				continue
			(n,hn,gn)=cc
			self.sons[item]=[int(n),int(hn),int(gn)]
		
	def hitNum(self):
		hasHit=0
		hit=0
		for n in self.sonNums:
			hit+=n*self.sonNums[n]
			if n > 0:
				hasHit+=self.sonNums[n]
		return (hasHit,hit)

	def hitRateStr(self):
		return getCountStr(self.num,self.sonNums)

	def pageRateStr(self):
		return getCountStr(self.num,self.pages)

	@staticmethod
	def printHead():
		return "点击数\t下拉数分布\t儿子数分布\t儿子点击情况\t有儿子点击数\t有儿子点击率\t儿子点击数\t平均儿子点击数\t有下拉数\t有下拉率\t下拉数\t平均下拉次数"
	
	def __str__(self):
		return "%d\t%s\t%s\t%s\t%s\t%s"%(self.num,self.pages,str(self.sonNums),self.sonStr(),self.hitRateStr(),self.pageRateStr())

	@staticmethod
	def read(cols):
		ret=PosRet()
		ret.num=int(cols[0])
		ret.pages=eval(cols[1])
		ret.sonNums=eval(cols[2])
		ret.readSons(cols[3])
		return ret

	def merge(self,other):
		self.num+=other.num
		DictUtil.merge(self.pages,other.pages)
		DictUtil.merge(self.sonNums,other.sonNums)
		for item in other.sons:
			if item not in self.sons:
				self.sons[item]=[0,0,0]
			self.sons[item][0]+=other.sons[item][0]
			self.sons[item][1]+=other.sons[item][1]
			self.sons[item][2]+=other.sons[item][2]

#要加上下拉页面的点击数目
def getSonTypes(id,actions):
	wrong=0
	hs={}
	for item in actions[id].resultItems():
		if item not in hs:
			hs[item]=[0,0,0]
		hs[item][0]+=1
	for sid in actions[id].sons:
		if sid >= len(actions):
			wrong+=1
			continue
		item=actions[sid].getItem()
		if item not in hs:
			sys.stderr.write(item+" of \n"+str(actions[sid])+"\nnot in \n"+str(actions[id])+"\n")
			wrong+=1
			continue
		hs[item][1]+=1
		if len(actions[sid].sons) > 0:
			hs[item][2]+=1
	for pid in actions[id].pull:
		if pid >= len(actions):
			wrong+=1
			continue
		(phs,pwr)=getSonTypes(pid,actions)
		for item in phs:
			if item not in hs:
				hs[item]=[phs[item][0],phs[item][1],phs[item][2]]
			else:
				hs[item][0]+=phs[item][0]
				hs[item][1]+=phs[item][1]
				hs[item][2]+=phs[item][2]
		wrong+=pwr
	return (hs,wrong)

def addTs(hs,sons):
	hit=0
	for item in hs:
		(n,hn,gn)=hs[item]
		hit+=hn
		if item not in sons:
			sons[item]=[0,0,0]
		sons[item][0]+=n
		sons[item][1]+=hn
		sons[item][2]+=gn
	return hit

def addAction(action,actions,ret):
	ret.num+=1
	DictUtil.addOne(ret.pages,len(action.pull))
	(hs,wrong)=getSonTypes(action.id,actions)
	hit=addTs(hs,ret.sons)
	DictUtil.addOne(ret.sonNums,hit)

def output(actions):
	if len(actions) == 0:
		return 0
	version=actions[0].user.version
	if version not in cs:
		cs[version]={}
	for action in actions:
		offset=column.getValue(action.para,"offset")
		if offset != "" and offset != "0": #下拉页不单独统计
			continue
		stype=hitItemName.getPosName(action)
		if stype not in cs[version]:
			cs[version][stype]=PosRet()
		ret=cs[version][stype]
		addAction(action,actions,ret)

def count():
	allCount=PosRet()
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
			cstr=str(cs[version][stype])
			if len(stype) == 0:
				stype="__VOID__"
			print "%s\t%s\t%s"%(stype,version,cstr)

def readLine(line):
	cols=line.strip().split("\t")
	version=cols[0]
	stype=cols[1]
	sret=HitRet.read(cols[2:])
	return (version,stype,sret)

def outputMerge(last,cs,allRet):
	for version in cs:
		print "%s\t%s\t%s"%(last,version,cs[version])
	print "%s\tall_version\t%s"%(last,allRet)

def merge():
	s="项目\t版本"
	s+=PosRet.printHead()
	print s
	last=""
	cs={}
	allRet=PosRet()
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) < 3:
			sys.stderr.write(line)
			continue
		stype=cols[0]
		if last == "":
			last=stype
		if last != stype:
			outputMerge(last,cs,allRet)
			last=stype
			cs={}
			allRet=PosRet()
		version=cols[1]
		if version not in cs:
			cs[version]=PosRet()
		try:
			int(cols[2])
		except:
			sys.stderr.write(line)
			continue
		sret=PosRet.read(cols[2:])
		cs[version].merge(sret)
		allRet.merge(sret)
	if last != "":
		outputMerge(last,cs,allRet)

if __name__=="__main__":
	if len(sys.argv) >= 2:
		if sys.argv[1] == "merge":
			merge()
	else:
		count()


