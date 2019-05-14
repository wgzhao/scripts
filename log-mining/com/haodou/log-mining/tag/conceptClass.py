#encoding=utf-8

import sys
sys.path.append("./")
import traceback

'''
#基于模板构建分类集合
#有几种类型的分类：一种包含的分类（这个不考虑界限问题），一种是精确的分类（要求描述的内容应该都包含在本概念之内）
#其中，精确的分类分两种情况，第一种是连续的精确包含，一种是非连续的精确包含。连续的精确包含要求所有内容是前后相接的，后者可以是内容分散在文中中的不同地方。
我们从包含的分类开始。对于包含的分类，由外部规则来切割文本，然后判定切割的部分是否具有某个概念属性。
我们可以对判定后的内容继续切割，看各个子部分是否具有某个概念。
这时候，我们就可以对于内部有哪些部分不能推导出本概念，哪些能推导出来。哪些部分单独不能推导出本概念，但是个合并起来可以推导出本概念。

'''


def construct(patternFile,dataFile=None):
	'''
		先要开发一套快速构建和评估训练集的方法。
		用现有的模板来快速为一个新概念构建训练集。
		
		将训练集的构建过程做成一个模板匹配的过程。
		不过，因为模板不匹配上下文，所以，在抽训练集时，要将上下文特别标识出来。
		因为识别新的模式时，这些上下文则成了关键。

		为了便于后期获取上下文，我们对于训练集，只标识文档id，以及文段在文档中的偏移，这样便于提取上下文。

		模板中的概念定义会在一个公共存储空间中。

	'''
	if dataFile == None:
		bank=RecipeBank()
	else:
		bank=RecipeBank(dataFile)
	patterns=readPatterns(patternFile)
	while True:
		recipe=bank.readRecipe()
		if not recipe:
			break

#
#召回可行的模板，有些模板只是召回模板，召回后，仍需要配套的模型来予以判定
#也就是说，需要配套的判定模型一起才能发挥作用
#
#可以用一个trie树来召回模板。
def recallPatterns(concept,doc,pos):
	return []

#
#与召回模板配套使用的判定模块。判定模块应该与召回模板合并呢，
#还是独立使用呢。这个要看判定模块是不是与召回模板是否一一对应。
#显然无需一一对应，因为召回模板是看是否有包含该概念的可能。召回模板还可以不召回整个概念，只召回部分要素。
#判定模板则需看所召回的概念是否与上下文协调一致。
#
#判定模块侧重于检查约束条件。而这些约束条件甚至不一定只与本概念有关。
#如与模板中子概念相关的约束。
#概念与文段的中心的关系。与模板外的元素的关系。
#[要为模板以外的元素分配角色。比如，“不”不做动词，也不做名词，这样的基本成分。但是，“不”和名词动词的识别存在明显的关联关系。要将这种关联关系进行分类。“不”后面不能接名词，但是可以接动词。]
def judge(concept,doc,pos,ret)
	return False

def classify(concept,doc,pos):
	'''
	分析一个doc中的某些元素是否为某个概念。
	'''
	patterns=recallPatterns(concept,doc,pos)
	ret=[]
	for pattern in patterns:
		mret=pattern.match(concept,doc,pos)
		ret.append((pattern,mret))
	return judge(concept,doc,pos,ret)

def test():
	pass


if __name__=="__main__":
	test()

