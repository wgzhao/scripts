#encoding=utf-8

from conf import *
import pattern

class Node(object):
	WildChildMark=0
	def __init__(self,w=None,parent=None):
		self.w=w
		self.parent=parent
		self.backChilds={}
		self.childs={}
		self.wilds={}
		self.targets={}

	def getChild(self,ei):
		#print "add ei:",en(ei)
		if ei not in self.childs:
			self.childs[ei]=Node(ei,self)
		return self.childs[ei]

	def targetStr(self,prefix):
		s=""
		for key in self.targets:
			s+="%s\t%s"%(prefix,str(key))
		return s

	def prefixStr(self,prefix):
		s=""
		for ei in self.childs:
			node=self.childs[ei]
			if type(ei) == unicode:
				sei=en(ei)
			else:
				sei=str(ei)
			s+="%s\tei:%s\tchild:%s\n"%(prefix,sei,node.prefixStr(prefix+"\t"))
		for ei in self.wilds:
			node=self.wilds[ei]
			sei=str(ei)
			s+="%s\tei:%s\tchild:%s\n"%(prefix,sei,node.prefixStr(prefix+"\t"))
		s+=self.targetStr(prefix)
		return s

	def addTarget(self,key,p):
		self.targets[key]=p

	def __str__(self):
		return self.prefixStr("")
		
	def matchUnicode(self,str,e):
		#print "str",en(str)
		if len(str) <= e:
			return (e,self)
		if str[e] in self.childs:
			tmpE=e+1
			tmpE,node=self.childs[str[e]].matchUnicode(str,tmpE)
			if len(node.targets) != 0:
				return tmpE,node
		return e,self

	def match(self,str):
		ustr=un(str)
		ret=[]
		i=0
		while len(ustr) > i:
			e,node=self.matchUnicode(ustr,i)
			#最大匹配
			if len(node.targets) > 0 and e > i:
				matched=ustr[i:e]
				ret.append((matched,node))
				i=e
			else:
				i+=1
		return ret
	
	def addPattern(self,p):
		node=self
		if type(p) == pattern.Pattern:
			if p.deny or p.type == pattern.Pattern.Concept:
				node=node.getChild(str(p))
			else:
				for e in p.es:
					node=node.addPattern(e)
			if p.objectConcept != None:
				node.addTarget(p.objectConcept,p)
		else:
			for ei in p:
				node=node.getChild(ei)
		return node
	
	def wstr(self):
		if self.w == None:
			return ""
		return ustr(self.w)

	def parentStr(self):
		s=""
		node=self
		while node != None:
			s=node.wstr()+s
			node=node.parent
		return s
		
class Trie(object):
	def reset(self,unstr=""):
		self.chart={}
		self.ws={}
		self.backWs={}
		self.unstr=unstr
	
	MaxSize=10000
	def __init__(self,file=None,otherFiles=[]):
		self.root=Node()
		self.reset()
		files=[]
		if file != None:
			files.append(file)
		for f in otherFiles:
			files.append(f)
		for f in files:
			for line in open(f):
				cols=line.strip().split("\t")
				if len(cols) < 2:
					sys.stderr.write(line)
					continue
				for c in cols[1:]:
					p=pattern.Pattern(objectConcept=c)
					p.readPattern(cols[0],False)
					self.root.addPattern(p)

	#w是目标概念，p是模板，i是起点，j是终点
	#ids是子串起始位置标识
	#
	#本函数的目标是保存结果
	def addW(self,w,p,i,j,ids): 
		if i not in self.ws: #相同起点的概念放在一起
			self.ws[i]={}
		lastJ=-1
		if w in self.ws[i]: 
			lastJ,lastP,lastIds=self.ws[i][w]
		if j > lastJ: #如果在该起点已经有概念，而当前概念的终点大于已经存有概念的终点，则替换原概念
			self.ws[i][w]=(j,p,ids)
			return True
		#print "lastJ",lastJ,"j",j,"w",ustr(w)
		return False
	
	#
	#本函数的目标是保存中间结果
	def addChart(self,node,i,s): #node匹配到字符串的起点s，终点i
		cs=[] #匹配到的目标概念
		if i not in self.chart:
			self.chart[i]={}
		lastS=Trie.MaxSize*100
		if node in self.chart[i]:
			lastS=self.chart[i][node]
		if s < lastS: #只保留覆盖范围最大的那个
			self.chart[i][node]=s
			return True
		return False

	class ConceptNode(object):
		def __init__(self,s,e,concept,sons=[]):
			self.s=s
			self.e=e
			self.concept=concept
			self.sons=sons
		
		@staticmethod
		def merge(cn1,cn2,concept=None):
			cn=Trie.ConceptNode(cn1.s,cn2.e,concept,sons=[])
			if cn1.e != cn2.s:
				raise Exception("cn1.e != cn2.s")
			if cn1.concept == None: #子串不构成独立的概念，则将子串的子串加入
				cn.sons.extend(cn1.sons)
			else:
				cn.sons.append(cn1)
			if cn2.concept == None:
				cn.sons.extend(cn2.sons)
			else:
				cn.sons.append(cn2)
			return cn
			

		def getStr(self,fix="\n\t"):
			s="{%s:(%d,%d)"%(ustr(self.concept),self.s,self.e)
			if self.sons != None and len(self.sons) > 0:
				s+=fix+"son:["
				ni=0
				for son in self.sons:
					if ni > 0:
						s+=fix
					s+=son.getStr(fix+"\t")
					ni+=1
				s+="]"
			s+="}"
			return s
		
		def __str__(self):
			return self.getStr()
		
		def find(self,target,ss=None):
			if ss == None:
				ss=[]
			if target == ustr(self.concept):
				ss.append(self)
				return ss
			for son in self.sons:
				son.find(target,ss)
			return ss

	def chartParser(self,str):
		unstr=un(str)
		self.reset(unstr=unstr)
		for i in range(len(unstr)):
			w=unstr[i]
			self.addChart(self.root,i,i)
			tws=[(w,None,i,i+1,"[%d,%d]"%(i,i+1))]
			while len(tws) > 0: #tws中所有元素共享终点tj=i+1
				(tw,tp,ti,tj,ids)=tws[-1]
				del tws[-1]
				for node in self.chart[ti]:
					if tw not in node.childs:
						continue
					s=self.chart[ti][node] #当前节点node在字符串上的起点为s，终点为ti
					child=node.childs[tw] #向后推进到子节点，子节点的起点就是node的终点，为ti
					if not self.addChart(child,tj,s): #chid与node的起点相同，都是s
						continue
					for concept in child.targets: #从子节点中读取目标概念
						p=child.targets[concept]
						newIds="[%d,%s]"%(s,ids)
						if not self.addW(concept,p,s,tj,newIds):
							continue
						tws.append((concept,p,s,tj,newIds))  #保存所得的目标概念到工作栈，标明起模板，在字符串上的起点s，终点从ti推进到tj

	def addChartNew(self,node,cn): #node匹配到字符串的起点s，终点i
		i=cn.e
		s=cn.s
		if i not in self.chart:
			self.chart[i]={}
		lastS=Trie.MaxSize*100
		if node in self.chart[i]:
			lastCn=self.chart[i][node]
			lastS=lastCn.s
		if s < lastS: #只保留覆盖范围最大的那个
			self.chart[i][node]=cn
			return True
		return False

	def addWNew(self,p,cn):
		i=cn.s
		j=cn.e
		w=cn.concept
		if i not in self.ws: #相同起点的概念放在一起
			self.ws[i]={}
		lastJ=-1
		if w in self.ws[i]:
			lastP,lastCn=self.ws[i][w]
			lastJ=lastCn.e
		if j > lastJ: #如果在该起点已经有概念，而当前概念的终点大于已经存有概念的终点，则替换原概念
			self.ws[i][w]=(p,cn)
			return True
		return False

	def chartParserNew(self,str):
		unstr=un(str)
		self.reset(unstr=unstr)
		for i in range(len(unstr)):
			w=unstr[i]
			self.addChartNew(self.root,Trie.ConceptNode(i,i,None))
			tws=[(w,Trie.ConceptNode(i,i+1,w))]
			while len(tws) > 0: #tws中所有元素共享终点tj=i+1
				(tp,cn)=tws[-1]
				tw=cn.concept
				ti=cn.s
				tj=cn.e
				del tws[-1]
				for node in self.chart[ti]:
					if tw not in node.childs:
						continue
					lastCn=self.chart[ti][node] #当前节点node在字符串上的起点为s，终点为ti
					s=lastCn.s
					mergeCn=Trie.ConceptNode.merge(lastCn,cn)
					child=node.childs[tw] #向后推进到子节点，子节点的起点就是node的终点，为ti
					if not self.addChartNew(child,mergeCn): #chid与node的起点相同，都是s
						continue
					for concept in child.targets: #从子节点中读取目标概念
						p=child.targets[concept]
						newCn=Trie.ConceptNode.merge(lastCn,cn,concept)
						if not self.addWNew(p,newCn):
							continue
						tws.append((p,newCn))  #保存所得的目标概念到工作栈，标明起模板，在字符串上的起点s，终点从ti推进到tj

	def __str__(self):
		s=""
		for i in self.ws:
			for w in self.ws[i]:
				p,cn=self.ws[i][w]
				j=cn.e
				if p != cn.concept:
					s+="\n%s-->%s(%d,%d)(%s)(%s)"%(ustr(w),ustr(self.unstr[i:j]),i,j,str(p),str(cn))
		for i in self.chart:
			for node in self.chart[i]:
				cn=self.chart[i][node]
				#s+="\nchart:%s-->%s(%d,%d)"%(node.parentStr(),ustr(self.unstr[start:i]),start,i)
		return s

	def select(self,target):
		ss=[]
		for i in self.ws:
			for w in self.ws[i]:
				p,cn=self.ws[i][w]
				j=cn.e
				if str(w) == target:
					ss.append(cn)
					#ss.append("\n%s-->%s(%d,%d)(%s)(%s)"%(ustr(w),ustr(self.unstr[i:j]),i,j,str(p),str(cn)))
		return ss

def test():
	tree=Trie(file="./mergeCateName.txt")
	f_handler=open('out.log', 'w')
	sys.stdout=f_handler
	#print tree.root
	for line in open("./test0922.txt"):
		line=line.strip()
		tree.chartParserNew(line)
		print line
		print tree		
	#for tw in tree.root.childs:
	#	print "child of root",ustr(tw)

if __name__=="__main__":
	test()


