#encoding=utf-8
#增量计算
#增量输入：A B
#历史累积：A B c intv
#
#对于每个A，保留v值排在top的1000个B，在这1000个B中：
#   优先从增量数据中取n个，然后取历史中的1000-n个
#
#
#n的计算：
#   如果有x个A，则：
#       n=50   if x > 100
#       n=(x+10)/2  if x > 10 and x <= 100
#       n=x         if x <= 10
#
#
# 如果B是菜谱，则要进行idf加权，按菜谱次数进行idf加权
# 如果B不是菜谱，则按其df=1计算idf值
#
# 如果B是搜索词，要考虑其长度。长度越长，则权重越大。

AN=1000
NewMax=30
NewMin=10

import sys
sys.path.append("./")

rs={}
for line in open("quality.df"):
	cols=line.strip().split("\t")
	rid=cols[0]
	vc=int(cols[-1])
	rs[rid]=vc

import math
def idf(B):
	df=1.0
	if B in rs:
		df+=rs[B]
		#sys.stderr.write(B+"\t"+str(df)+"\n")
	elif B.startswith("rid-") and B[4:] in rs:
		df+=rs[B[4:]]
		#sys.stderr.write(B+"\t"+str(df)+"\n")
	else:
		df+=1000.0/(len(B)+0.1)  #关键词长度越长，被认为越稀有
	if df < 2:
		df=2
	return 1.0/math.log(df)

def newSize(x):
	if x > NewMax*2:
		return NewMax
	if x > NewMin:
		return (x+NewMin)/2
	return x

def idfWeit(d):
	nd={}
	for w in d:
		nd[w]=math.log(d[w]+1)*idf(w)
	return nd

def topn(d,n):
	if len(d) <= n:
		return sorted(d.items(), key=lambda x: x[1],reverse=True)
	return sorted(d.items(), key=lambda x: x[1],reverse=True)[0:n]

ScaleFac=127


def output(lastA,addDict,allDict):
	n=newSize(len(addDict))
	nd=idfWeit(addDict)
	list=topn(nd,n)
	max=0
	if len(list)>0:
		max=list[0][1]
	for B,v in list:
		#sys.stderr.write(lastA+"\t"+B+"\t"+str(addDict[B])+"\t"+str(nd[B])+"\tn\n")
		c=allDict[B]
		del allDict[B]
		print lastA+"\t"+B+"\t"+str(c)+"\t"+str(int(ScaleFac*nd[B]/max))+"\tn"
	n=AN-n
	nd=idfWeit(allDict)
	list=topn(nd,n)
	if len(list)>0:
		if list[0][1] > max:
			max=list[0][1]
	for B,v in list:
		#sys.stderr.write(lastA+"\t"+B+"\t"+str(allDict[B])+"\t"+str(nd[B])+"\n")
		print lastA+"\t"+B+"\t"+str(allDict[B])+"\t"+str(int(ScaleFac*nd[B]/max))
	

lastA=""
addDict={}
allDict={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	A=cols[0]
	if lastA == "":
		lastA=A
	if lastA != A:
		output(lastA,addDict,allDict)
		lastA=A
		addDict={}
		allDict={}
	if len(cols) < 2:
		sys.stderr.write("columns of line < 2:"+line+"\n")
		continue
	B=cols[1]
	if len(cols) == 2:
		if B not in addDict:
			addDict[B]=1
		else:
			addDict[B]+=1
	v=1
	if len(cols) >= 3:
		v=int(float(cols[2]))
	if B not in allDict:
		allDict[B]=v
	else:
		allDict[B]+=v
if lastA != "":
	output(lastA,addDict,allDict)


