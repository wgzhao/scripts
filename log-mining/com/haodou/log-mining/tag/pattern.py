#encoding=utf-8

import sys

#
#把工作空间中的内容加载进来，直接读取
#
from workBench import *
sys.path.append("../util")
from utf8 import *

from seg import *

eleProp={}

def readPattern():
	pass

def dictMatch(tree,line):
	pass

def eleOf(ele,set):
	if ele == set:
		return True
	if type(set) == list or type(set) == dict:
		return ele in set
	return False

def propOf(ele,w):
	if w == ele:
		return True
	if w not in eleProp:
		return False
	for prop in eleProp[w]:
		if prop == ele:
			return True
	return False

#
#这个视为一个分类问题。
#
def matchEle(ele,str,start=0):
	if type(str) != unicode:
		str=str.decode("utf-8")
	if type(ele) != unicode:
		ele=ele.decode("utf-8")
	s=str.find(ele)
	if s > 0:
		return (s,s+len(ele))
	i=start
	size=len(str)
	for i in range(0,size,1):
		for j in range(i+1,size,1):
			w=str[i:j]
			lwc.add(w)
			if not lwc.inFreq(w):
				break
			if propOf(ele,w):
				return (i,i+len(w))
	return (s,s+len(ele))

#对模板进行计数，当一个模板的计数超过一定阈值，且模板在父模板中的比重超过一定阈值（父模板的元素是子模板的子集）
#这时候，可以对模板进行扩展生成子模板。
#
#一个模板中包含多个要素（槽位），同一个要素可以被归为一类。
#
#对要素的限定严格化。何时进行严格化操作。
#严格化了之后，不仅会影响本模板，也会影响共享此要素的模板的计数。
#共享要素：并集共享，还是交集共享呢。如果是并集共享，则可以提高模板的泛化能力，否则生成更多的模板。
#如果是严格化要素，可是实施交集共享。将非交集部分从模板中去掉。
#但是，如果需要提高泛化能力，则实施并集共享。
#
class Pattern(object):
	def __init__(self):
		self.es=[]
		#元素之间的位置依赖,pos可以视为一种特殊的属性依赖
		self.elePos=[]
		#元素之间的属性依赖
		self.eleRele=[]
		self.eleProp={}
		#模板计数
		self.c=0

	def match(self,str):
		has=True
		for i in range(len(self.es)):
			ele=self.es[i]
			(s,end)=matchEle(ele,str)
			if s < 0:
				has=False
		#没有匹配上也不退出，因为可以统计一些别的有用信息
			if i not in self.eleProp:
				self.eleProp[i]={}
			self.eleProp[i]["pos"]=(s,e)
		if not has:
			return False
		self.c+=1
		return has

#给定文本，对文本进行pattern的匹配
#
#每扫描到一个给定字符串，则看对其怎么扩展。其一，考虑其有没有更长的检索串。其二，考虑其有没有长距离约束。
#其三，考虑其有没有泛化扩展(属性扩展）。
#一个概念会有很多属性，但是，并不是所有的属性都会被触发为检索属性。
#但在某些情况下，需要对概念的属性进行穷举，这时候，所有的属性都成了检索属性。
#
#总是运行在一个更高的空间，这个更高的空间有必要选择合适的层级。
#目前写的这些底层脚本也是有用的，他们可以独立运行。
#但是，在更高的空间中，要对这些底层脚本进行整合。
#
def match(doc):
	proName="模式匹配扫描代码"
	if proName not in concepts:
		concepts[proName]={}

#用一个proName指示怎么随着数据的变化而改变代码逻辑
##扫描代码过程本身也要随着工作空间的数据变化而变化的。
#上面这样一个简单的处理，就可以让程序步骤受工作空间数据的影响。
#
#而且，程序在执行过程中的步骤信息，也要及时加到工作空间中。
#


#
#这些索引要加入到分词器中，这样分词结果马上能够得到索引项
#这个是在设立索引的环节，为pattern设立多样化的索引，保证pattern能够在多种形式下命中
#
def patternVari(pattern):
	pass


def test():
	for line in sys.stdin:
		line=un(line.strip())
		ws=MaxSeg.getSeg().maxSeg(line)
		for w in ws:
			print en(w)


if __name__=="__main__":
	test()

