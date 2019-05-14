#encoding=utf-8

import sys
import math

sys.path.append("./")

from conf import *
import Stack
#import trie

#词1出现a次，词2出现b次，共现c次，共有n个词语
#则相关系数为
def rela(a,b,c,n):
	return math.pow(a*b/(a+5.0)/(b+5.0),0.5)*(n*c-a*b)/math.pow((n+1-a)*a,0.5)/math.pow((n+1-b)*b,0.5)

#
#设立一个简单的模式的结构
#并写一个匹配算法
#

class Pattern(object):
	Concept=PatternConcept
	Default=PatternDefault
	Context=PatternContext
	Deny=PatternDeny
	concepts={}  #记录每个概念的覆盖范围最小最大值，指导搜索范围

	starts={
		u"[":u"]",
		u"<":u">",
		u"{":u"}"
	}
	ends={
		u"]":u"[",
		u"}":u"{",
		u">":u"<"
	}

	def __init__(self,type=Default,objectConcept=None):
		'''
		self.contents=[]  #内容元素集合
		self.context=[]  #上下文元素结合
		'''
		self.es=[]      #所有元素集合
		self.type=type
		self.deny=False
		self.objectConcept=objectConcept #本模板所实现的目标概念

	def simpleIndex(self):
		return str(self.es[0])
	
	def setIndexFunc(self,funcStr):
		self.indexFunc=eval(funcStr)

	#
	#第i个子元素的属性
	def propOfE(self,i):
		return self.es[i][u"prop"]
	
	#
	#模板过滤的几个要素，首先，要建立模板的索引。当文本中出现索引时，就会触发对模板的匹配。
	#索引可以是布尔型表达式： "老师 | （学生 & 书本)"
	#
	#trie树只能用字符串索引。但是，对于模板检索而言，其他类型的索引也是可以的。
	#模板本身可以作为索引项。也可以做成trie索引。
	#
	#虽然trie树暗含索引项之间的先后顺序，但在实际匹配时可以不用有先后顺序。
	#做到这一点，只需要把前面刚匹配过的因素存储到一个池子中就可以，只要后面可以激活。
	#不过有个问题，在激活的时候，会有一个n^2的匹配复杂度，非线性。这里的n是文本的长度。
	#
	#不过通过控制每个索引节点的非顺序子索引项的数量，就可以不用遍历文本池子，而是遍历非顺序子索引项，这样降低复杂度。
	#解决办法：对于有m个子索引的非顺序索引，给出m*(m-1)的顺序索引，对于剩下的m-2项子索引，以非顺序的方式出现。
	#或者用赫夫曼编码的思路，将高频的子索引组合编制成顺序索引，剩余子索引以非顺序的方式出现。
	#对于剩余的非顺序子索引，不能为每个子索引建一个单独的节点，而是把它们放到一个节点里面。
	#
	#符号：用+表示顺序索引，用&表示非顺序索引，用|表示索引组合。
	#+并不表示索引项之间必须前后连接，如果两个项目必须前后连接，在两个项目之间啥符号也没有就好。
	#
	#其实，非顺序索引本身已经不是索引了，而是筛选条件。
	def getIndex(self):
		return self.indexFunc(self)

	def add(self,p):
		if type(p) == unicode and p == Pattern.Deny:
			self.deny=(not self.deny)
			return
		if type(p) == Pattern:
			#print "type of p",type(p)
			if self.deny:
				p.deny=(not p.deny)
		if self.type == Pattern.Concept and len(self.es) > 0:
			self.es[0]+=p
			return
		self.es.append(p)

	#读取字符串模式
	def readPattern(self,str,readContext=True):
		us=un(str.strip())
		s=Stack.Stack()
		for c in us:
			(index,pa)=s.get()
			if c in Pattern.starts:
				pi=Pattern(type=c)
				s.add(pi)
			elif c in Pattern.ends:
				if pa == None or pa.type != Pattern.ends[c]:
					pi=c
				else:
					s.pop()
					continue
			else:
				pi=c
			#忽略pattern中的上下文，因为chart解析不用考虑这个
			if not readContext and type(pi) == Pattern and pi.type == Pattern.Context:
				continue
			if pa != None:
				pa.add(pi)
			else:
				self.add(pi)
	
	#
	#如果是否定模式，则需要为本模式取通配符的模式
	#
	def getLen(self):
		if self.type == Pattern.Concept:
			c=self.es[0]
			if c not in Pattern.concepts:
				return (1,20)
			return Pattern.concepts[c]
		else:
			min=0
			max=0
			for e in self.es:
				if type(e) == Pattern:
					(emin,emax)=e.getLen()
					max+=emax
					min+=emin
				else:
					max+=len(e)
					min+=len(e)
				return (min,max)
		return (min,max)


	def __str__(self):
		s=en(self.type)
		if self.deny:
			s+=en(Pattern.Deny)
		#s+="len:%d"%(len(self.es))
		for e in self.es:
			if type(e) == unicode:
				s+=en(e)
			else:
				s+=str(e)
			#print "s",s
		return s+en(self.starts[self.type])


def testReadPattern():
	#for line in open("/home/zhangzhonghui/log-mining/com/haodou/log-mining/log/mergeCateName.txt"):
	for line in open("./num.txt"):
		cols=line.strip().split("\t")
		p=Pattern(objectConcept=cols[1])
		p.readPattern(cols[0],False)
		print p

if __name__=="__main__":
	testReadPattern()

	
