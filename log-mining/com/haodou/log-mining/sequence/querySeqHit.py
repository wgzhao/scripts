#encoding=utf-8
import sys
import seqHit
import os

sys.path.append("../")
import DictUtil

import hitItemName

def getSortFiles():
	sortedFiles=[]
	files=os.listdir("/home/zhangzhonghui/data/seqHit/")
	for file in sorted(files):
		if not file.startswith("20"):
			continue
		sortedFiles.append("/home/zhangzhonghui/data/seqHit/"+file)
	return sortedFiles

'''
def reportSons(method,version,v2=100000):
	ts={}
	dts=[]
	files=getSortFiles()
	for file in files:
'''

def reportSonsFile(method,version,file,ts,dts,v2=100000):
	if True:
		for line in open(file):
			if line.startswith("版本"):
				continue
			if True:
				try:
					(v,stype,sret)=seqHit.readLine(line)
				except:
					sys.stderr.write(line)
					continue
				tnum=sret.num+1e-32
				if tnum < 1.0:
					continue
				try:
					v=int(v)
				except:
					v=0
				if v >= version and v <= v2 and stype.startswith(method):
					if len(dts) <= 0 or dts[-1][0] != file:
						dts.append((file,{}))
					for type in sret.sonTypes:
						rate=DictUtil.sum(sret.sonTypes[type])
						type=hitItemName.getTypeName(type)
						if type not in dts[-1][1]:
							ts[type]=1
							dts[-1][1][type]=0
						dts[-1][1][type]+=rate
	'''
	s="日期"
	for type in ts:
		s+="\t"+type
	print s
	for file,dt in dts:
		day=file[file.rfind("/")+1:]
		s=day
		for type in ts:
			if type not in dt:
				s+="\t0"
			else:
				s+="\t%r"%(dt[type])
		print s
	'''

def reportSons(method,version,v2=100000):
	ts={}
	dts=[]
	files=getSortFiles()
	for file in files:
		reportSonsFile(method,version,file,ts,dts,v2=100000)
	s="日期"
	for type in ts:
		s+="\t"+type
	print s
	for file,dt in dts:
		day=file[file.rfind("/")+1:]
		s=day
		for type in ts:
			if type not in dt:
				s+="\t0"
			else:
				s+="\t%r"%(dt[type])
		print s

recipeInfo={
	"info.getinfo":1,
	"info.getinfov2":1,
	"info.getinfov3":1,
	"info.getlastestinfo":1,
	#"topic.getinfo":1,    #专辑和话题的收藏接口不同
	#"info.getalbuminfo":1,
}

recipeFavMethod="favorite.addrecipe"
#recipeFavMethod="like.add"

def favStr(num,hnum,favNum):
	return "\t%d\t%d\t%d\t%.4f\t%.4f\t%.4f"%(num,hnum,favNum,hnum/(num+1e-32),favNum/(hnum+1e-32),favNum/(num+1e-32))

def readFavStr(cols):
	return (int(cols[0]),int(cols[1]),int(cols[2]))

#
#计算收藏比例
def sonFavRate():
	print "日期\t版本\t栏目名\t栏目进入数\t列表页菜谱点击数\t菜谱收藏数\t菜谱点击率\t菜谱收藏率\t栏目菜谱收藏率"
	files=os.listdir("/home/zhangzhonghui/data/seqHit/")
	for file in sorted(files):
		if not file.startswith("20"):
			continue
		day=file[file.rfind("/")+1:]
		ts={}
		for line in open("/home/zhangzhonghui/data/seqHit/"+file):
			if line.startswith("版本"):
				continue
			(v,stype,sret)=seqHit.readLine(line)
			selfNum=0
			for st in sret.sonTypes:
				if st in recipeInfo:
					selfNum+=DictUtil.sum(sret.sonTypes[st])
			if selfNum > 0:
				if recipeFavMethod in sret.gsonTypes:
					favNum=DictUtil.sum(sret.gsonTypes[recipeFavMethod])
					if stype not in ts:
						ts[stype]=[0,0,0]
					ts[stype][0]+=sret.num
					ts[stype][1]+=selfNum
					ts[stype][2]+=favNum
					print day+"\t"+v+"\t"+stype+favStr(sret.num,selfNum,favNum)
#"\t%d\t%d\t%d\t%.4f\t%.4f\t%.4f"%(sret.num,selfNum,favNum,selfNum/(sret.num+1e-32),favNum/(selfNum+1e-32),favNum/(sret.num+1e-32))
		for stype in ts:
			(num,hnum,favNum)=ts[stype]
			print day+"\tall_version\t"+stype+favStr(num,hnum,favNum)

def favRate():
	files=os.listdir("/home/zhangzhonghui/data/seqHit/")
	for file in sorted(files):
		if not file.startswith("20"):
			continue
		for line in open("/home/zhangzhonghui/data/seqHit/"+file):
			if line.startswith("版本"):
				continue
			(v,stype,sret)=seqHit.readLine(line)
			if stype not in recipeInfo:
				continue
			selfNum=sret.num
			if recipeFavMethod in sret.sonTypes:
				favNum=DictUtil.sum(sret.sonTypes[recipeFavMethod])
				print file+"\t"+v+"\t"+stype+"\t%d\t%d\t%.4f"%(selfNum,favNum,favNum/(selfNum+1e-32))

#
#读取sonFavRate()的输出结果
#
def favRateCompare(name=None):
	ts={}
	dts={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if cols[1] != "all_version":
			continue
		day=cols[0]
		stype=cols[2]
		if name == None:
			stype=hitItemName.getTypeName(cols[2])
		else:
			if stype.find(name) < 0:
				continue
		(num,hnum,favNum)=readFavStr(cols[3:])
		#favNum=hnum  #计算点击率
		#hnum=num
		rate=favNum/(hnum+1e-32)
		if stype not in ts:
			ts[stype]=1
		if day not in dts:
			dts[day]={}
		if stype not in dts[day]:
			dts[day][stype]=[hnum,favNum]
		else:
			dts[day][stype][0]+=hnum
			dts[day][stype][1]+=favNum
	s="日期"
	for stype in ts:
		s+="\t"+stype
	print s
	for day in dts:
		s=day
		for stype in ts:
			rate=0
			if stype in dts[day]:
				rate=dts[day][stype][1]/(dts[day][stype][0]+1e-32)
			s+="\t%.4f"%(rate)
		print s

def searchKeyHit():
	keywordMethods={
		"search.getlist_k":1,
		"search.getlist_k1":1,
		"search.getlist_k2":1,
		"search.getlist_k3":1,
		"search.getlistv3_k":1,
		"search.getsearchindex_k":1,
		"search.getsearchindex_k1":1,
	}
	
	files=getSortFiles()
	s="日期\t搜索量"
	for stype in keywordMethods:s+="\t"+stype
	s+="\t有点击数\t有点击率\t点击数\t平均点击数"
	print s
	for file in files:
		dret=seqHit.HitRet()
		ds={}
		for stype in keywordMethods:ds[stype]=0
		nn=0
		day=file[file.rfind("/")+1:]
		#if not day.startswith("2015-03-0"):
		#	continue
		for line in open(file):
			if nn == 0:
				nn+=1
				continue
			(v,stype,sret)=seqHit.readLine(line)
			#if v != "470":
			#	continue
			if stype not in keywordMethods:continue
			#print "%s\t%s\t%s\t%s"%(day,v,stype,sret.hitRateStr())
			if stype not in ds:
				ds[stype]=sret.num
			else:
				ds[stype]+=sret.num
			dret.merge(sret)
		s=day
		s+="\t%d"%(dret.num)
		for stype in keywordMethods:s+="\t%d"%(ds[stype])
		s+="\t"+dret.hitRateStr()
		print s

if __name__=="__main__":
	if sys.argv[1] == "sons":
		if len(sys.argv) >= 5:
			reportSons(sys.argv[2],int(sys.argv[3]),int(sys.argv[4]))
		else:
			#reportSons("视频菜谱",470,480)
			reportSons("recipe.getcollectlist",470,490)
			#reportSons("recipe.getcollectrecomment_私人定制",400,480)
	elif sys.argv[1] == "favRate":
		#favRate()
		sonFavRate()
	elif sys.argv[1] == "favRateCompare":
		if len(sys.argv) <= 2:
			favRateCompare()
		else:
			favRateCompare(sys.argv[2])
	elif sys.argv[1] == "searchKeyHit":
		searchKeyHit()


