#encoding=utf-8
import sys

class MoreSearch(object):
	def __init__(self):
		#更多菜谱点击数
		self.r=0
		#更多菜谱点击词数
		self.rb=0
		#搜索词下分类标签点击数
		self.rt={}
		self.rTagShow={}
		self.rtn=0
		#更多专辑
		self.a=0
		self.ab=0
		#更多话题
		self.t=0
		self.tb=0
	
	def merge(self,ms):
		if ms == None:
			return
		self.r+=ms.r
		self.rb+=ms.rb
		self.a+=ms.a
		self.ab+=ms.ab
		self.t+=ms.t
		self.tb+=ms.tb
		self.rtn+=ms.rtn
		for t in ms.rt:
			if t not in self.rt:
				self.rt[t]=ms.rt[t]
			else:
				self.rt[t]+=ms.rt[t]
		for t in ms.rTagShow:
			if t not in self.rTagShow:
				self.rTagShow[t]=ms.rTagShow[t]
			else:
				self.rTagShow[t]+=ms.rTagShow[t]

	def __str__(self):
		return "%d\t%d\t%d\t%d\t%d\t%d\t%s\t%s\t%d"%(self.r,self.rb,self.a,self.ab,self.t,self.tb,str(self.rt),str(self.rTagShow),self.rtn)
	
def readMoreSearch(cols):
	try:
		ms=MoreSearch()
		ms.r=int(cols[0])
		ms.rb=int(cols[1])
		ms.a=int(cols[2])
		ms.ab=int(cols[3])
		ms.t=int(cols[4])
		ms.tb=int(cols[5])
		ms.rt=eval(cols[6])
		if len(cols) >= 8:
			ms.rTagShow=eval(cols[7])
		if len(cols) >= 9:
			ms.rtn=int(cols[8])
		return ms
	except Exception,e:
		sys.stderr.write(str(e)+"\n")
		sys.stderr.write("error:"+line)
		return None

if __name__=="__main__":
	ms=MoreSearch()
	ms.r=100
	ms.rb=80
	ms.a=30
	ms.ab=28
	ms.t=20
	ms.tb=16
	ms.rt={}
	ms.rt["早餐"]=1
	ms.rt["懒人菜谱"]=2
	print "ms",ms
	for line in open("/home/zhangzhonghui/data/tt.1208"):
		tms=readMoreSearch(line.strip().split("\t")[2:])
		if tms == None:
			print "error:",line
			continue
		print "tms:",tms
		ms.merge(tms)
		print "mms:",ms

	
