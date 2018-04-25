# -*- coding: utf-8 -*-
"""
Created on Thu Nov 06 10:06:42 2014

@author: Yanghong
搜索关键字按Tag进行统计，统计各Tag分别占搜索的%等指标
"""

import sys
import codecs
import MySQLdb
import operator
	
reload(sys)   
sys.setdefaultencoding('utf8') 


#读入关键字和Tag的对应关系文件
def readCate(file):
	n2ps={}
	for line in codecs.open(file,"r","utf-8"):
		line=line.strip()
		if len(line) == 0:
			continue
		cols=line.split("\t")
		if len(cols) < 2: #只有关键词，没有对应的类别
			continue
		word=cols[0]
		if word=="_" : continue
		n2ps[word] = cols[1:] #一列表方式返回，主要是保存[类别]顺序的包含关系，
	return n2ps

#获得关键字对应的根分类
def getRootCate(nd):
	name2Root={}
	for k in nd:
		#cateName = nd[k][-1]
		cateName = nd[k][0]
		while(cateName in nd):
			#cateName = nd[cateName][-1]
			cateName = nd[cateName][0]
		name2Root[k] = cateName
	return name2Root

#从mysql数据库中加载表到字典
#加载的具体表由传入的sql参数指定
def loadTable(sql):
	Data={}
	try:
		conn=MySQLdb.connect(host='10.1.1.70',user='bi',passwd='bi_haodou',port=3306,charset='utf8')	#连接mysql数据库
		cursor = conn.cursor()
		cnt=cursor.execute(sql)
		#print u"加载记录数:"+repr(cnt)
		for i in range(cnt):
			key = cursor.fetchone()[0].decode('utf-8')
			if len(key.strip()) > 0:
				Data[key]=1
			else:
				continue
		cursor.close()
		conn.close()
		return Data
	except Exception,e:
		conn.close()
		print sql
		print e		
		return {}
	
#INIT_FLAG=False
#定义全局变量
name2Root={} #关键词分类定义文件
reciptData={} #菜谱数据，从数据库中加载
foodData={} #食材数据，从数据库中加载
foodCate={} #食材分类数据，从数据库中加载
shopData={} #店铺数据，从数据库中加载



#初始化
#用于加载各种数据
def initData(printFlat=True):
	global name2Root,reciptData,foodData,foodCate,shopData #声明全局变量
	if printFlat: print u"加载mergeCateName.txt文件..."	
	nd=readCate(r"./mergeCateName.txt") #加载name字典
	name2Root = getRootCate(nd)
	
	#从Mysql数据库加载Recipt表
	if printFlat: print u"加载recipt表..."
	sql = r"SELECT Title FROM haodou_recipe.Recipe  WHERE Title IS NOT NULL AND Status = 0;"
	reciptData = loadTable(sql)
	#print u"recipt表记录数："+ repr(len(reciptData))
	
	#从Mysql数据库加载食材表
	if printFlat: print u"加载FoodData表..."
	sql = r'SELECT a.`Name` FROM haodou_recipe.`FoodData` a ;' 
	foodData = loadTable(sql)
	
	#从Mysql数据库加载菜系表
	if printFlat: print u"加载FoodCate表..."
	sql = r'SELECT a.`CateName` FROM haodou_pai.`FoodCate` a ;' 
	foodCate =  loadTable(sql)
	
	#从Mysql数据库加载餐馆表
	if printFlat: print u"加载Shop表..."
	sql = r'SELECT a.Name FROM haodou.`Shop` a WHERE a.Name IS NOT NULL;' 
	shopData =  loadTable(sql)
	
	#INIT_FLAG = True #初始化完成后设置为True
	return

#获取关键词对应的Cate

def getCate(word):
	global name2Root,reciptData,foodData,foodCate,shopData #声明全局变量
	if len(name2Root)==0: initData() #如果没有初始化，则进行初始化，否则不再初始化
	try:
		if word in foodData:
			tag = u"[食材]"
		elif word in reciptData:
			tag = u"[菜谱]"
		elif word in foodCate:
			tag = u"[菜系]"
		elif word in shopData:
			tag = u"[餐馆]"
		elif word in name2Root:
			tag= name2Root[word]
		else:
			tag=u'[**无对应分类]'
	except Exception:
		tag=u'[**无对应分类]'
	return tag

	
#统计以下指标
#各类TAG的主动搜索数、主动搜索率、被动搜索数、被动搜索数、被动搜索率、搜索点击数，搜索点击率
#

WORD_ID=0
SEARCH_CNT=1 #搜索总次数
SEARCH_DOWN=2 #翻页
NO_TAG_ID=3 #不带TAG的搜索总次数
CLIK_ALL=4 #总的点击率

#根据忠辉文件定义的关系进行搜索TAG类别的统计
#第二个参数为统计的日期，用于加到输出文件名中
def searchTag(f,datestr=""):
	global name2Root,reciptData,foodData,foodCate,shopData #声明全局变量

	#初始化数据	
	initData()
	
	fDetail = open("./searchTagDetail"+datestr+".csv","w")
	tagCnt={}
	total = 0
	total_NoTagID = 0
	total_Click = 0
	for line in f:
		try:
			cols = line.strip().split('\t')
			#print cols
			if len(cols)<5:
				continue
			word = cols[WORD_ID].strip().decode('utf-8','ignore')
			if word =="_" :continue

			#获取一个词的Cate
			tag = getCate(word)
			#line = tag+'\t'+word+'\t'+cols[SEARCH_CNT]+'\n'
			line = tag+','+word+','+cols[SEARCH_CNT]+'\n'
			fDetail.write( line.encode("gbk",'ignore'))

			if tag not in tagCnt:
				tagCnt[tag] = {'SEARCH_CNT':int(cols[SEARCH_CNT]),'SEARCH_DOWN':int(cols[SEARCH_DOWN]),'NO_TAG_ID':int(cols[NO_TAG_ID]),'CLIK_ALL':int(cols[CLIK_ALL])}
			else:
				tagCnt[tag]['SEARCH_CNT'] += int(cols[SEARCH_CNT])
				tagCnt[tag]['SEARCH_DOWN'] += int(cols[SEARCH_DOWN])
				tagCnt[tag]['NO_TAG_ID'] += int(cols[NO_TAG_ID])
				tagCnt[tag]['CLIK_ALL'] += int(cols[CLIK_ALL])
			total += int(cols[SEARCH_CNT])
			total_NoTagID += int(cols[NO_TAG_ID])
			total_Click += int(cols[CLIK_ALL])
		except Exception,e:
			print e
			print word
			continue
	fDetail.close()
	#conn.close()

	
	#print u"类别"+'\t'+u"总搜索数"+'\t'+u"其中翻页数量"+'\t'+u"主动搜索数量"+'\t'+u"TAG搜索数量"+'\t'+u"点击数量"
	fout = open("./searchTag"+datestr+".csv","w")
	#line = u"类别"+'\t'+u"总搜索数"+'\t'+ u"占比%"+'\t'+u"其中翻页数量"+'\t'+u"平均页数"+'\t'+u"主动搜索数量"+'\t'+u"TAG搜索数量"+'\t'+u"点击数量"+'\t'+u"点击率"+'\n'
	line = u"类别"+','+u"总搜索数"+','+ u"占比%"+','+u"其中翻页数量"+','+u"平均页数"+','+u"主动搜索数量"+','+u"TAG搜索数量"+','+u"点击数量"+','+u"点击率"+'\n'
	fout.write(line.encode("gbk"))

	for tag in tagCnt:
		#print tag 
		#print tag +'\t' + repr(tagCnt[tag]['SEARCH_CNT']) +'\t' + repr(tagCnt[tag]['SEARCH_DOWN']) +'\t' + repr(tagCnt[tag]['NO_TAG_ID']) +'\t'+ repr((tagCnt[tag]['SEARCH_CNT'] -  tagCnt[tag]['NO_TAG_ID']))  +'\t' + repr(tagCnt[tag]['CLIK_ALL'])
		'''		
		line = tag +'\t' + repr(tagCnt[tag]['SEARCH_CNT']) +'\t'+ repr(round(float(tagCnt[tag]['SEARCH_CNT'])/float(total),2))+'\t' \
			+ repr(tagCnt[tag]['SEARCH_DOWN']) +'\t'+ repr(round(float(tagCnt[tag]['SEARCH_CNT'])/float(tagCnt[tag]['SEARCH_DOWN']),2)) \
			+'\t' + repr(tagCnt[tag]['NO_TAG_ID']) +'\t'+ repr((tagCnt[tag]['SEARCH_CNT'] -  tagCnt[tag]['NO_TAG_ID']))  +'\t' \
			+ repr(tagCnt[tag]['CLIK_ALL']) + '\t'+ repr(round(float(tagCnt[tag]['CLIK_ALL'])/float(tagCnt[tag]['SEARCH_CNT']),2))+'\n'
		'''
		line = tag +',' + repr(tagCnt[tag]['SEARCH_CNT']) +','+ repr(round(float(tagCnt[tag]['SEARCH_CNT'])/float(total),2))+',' \
			+ repr(tagCnt[tag]['SEARCH_DOWN']) +','+ repr(round(float(tagCnt[tag]['SEARCH_CNT'])/float(tagCnt[tag]['SEARCH_DOWN']),2)) \
			+',' + repr(tagCnt[tag]['NO_TAG_ID']) +','+ repr((tagCnt[tag]['SEARCH_CNT'] -  tagCnt[tag]['NO_TAG_ID']))  +',' \
			+ repr(tagCnt[tag]['CLIK_ALL']) + ','+ repr(round(float(tagCnt[tag]['CLIK_ALL'])/float(tagCnt[tag]['SEARCH_CNT']),2))+'\n'
			
		fout.write(line.encode("gbk"))
	#line = u"合计："+'\t'+repr(total)+'\t'+'\t'+'\t'+'\t'+repr(total_NoTagID)+'\t'+repr(total -total_NoTagID) +'\t'+repr(total_Click)+'\n'
	line = u"合计："+','+repr(total)+','+','+','+','+repr(total_NoTagID)+','+repr(total -total_NoTagID) +','+repr(total_Click)+'\n'
	fout.write(line.encode("gbk"))
	fout.close()
		

	
if __name__=="__main__":
	if len(sys.argv)>=2:
		datestr=sys.argv[1]
	searchTag(sys.stdin,datestr)
