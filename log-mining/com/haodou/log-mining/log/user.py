#encoding=utf-8
import sys

sys.path.append("./")
sys.path.append("../")

from column import *
import dictInfo

def count(f):
	lineNum=0
	uNum=0
	us={}
	nus={}
	uids={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < APP_LOG_COLUMNS4:
			continue
		lineNum+=1
		u=cols[USER_CID]
		uuid=cols[UUID_ID]
		if uuid not in uids:
			uids[uuid]=1
		else:
			uids[uuid]+=1
		if PositiveNumP.match(u):
			if u not in us:
				us[u]={}
			uNum+=1
			if uuid not in us[u]:
				us[u][uuid]=1
			else:
				us[u][uuid]+=1
		else:
			if uuid not in nus:
				nus[uuid]=1
			else:
				nus[uuid]+=1
	sum=0
	for u in us:
		sum+=len(us[u])
		if len(us[u]) >= 3:
			print u,us[u]
		v=0
		for uuid in us[u]:
			v+=us[u][uuid]
		us[u]=v
	print "注册用户请求日志行数统计信息"
	print dictInfo.info(us)
	print "非注册用户请求日志行数统计信息"
	print dictInfo.info(nus)
	print "注册用户日志行数\t总日志行数\t注册用户占比"
	print "%d\t%d\t%.4f"%(uNum,lineNum,float(uNum)/lineNum)
	print "总uuid数\t注册uuid+非注册uuid\t注册与非注册uuid重叠比例"
	print "%d\t%d\t%.4f"%(len(uids),len(nus)+sum,float(len(nus)+sum)/len(uids)-1.0)
	print "注册用户数\t注册uuid数\t注册用户多个uuid比例"
	print "%d\t%d\t%.4f"%(len(us),sum,float(sum)/len(us)-1.0)
	print "注册用户数\t注册用户日志行数\t注册用户平均行数"
	print "%d\t%d\t%.4f"%(len(us),uNum,float(uNum)/len(us))
	print "非注册uuid数\t非注册用户日志行数\t非注册用户平均行数"
	print "%d\t%d\t%.4f"%(len(nus),lineNum-uNum,float(lineNum-uNum)/len(nus))
	
if __name__=="__main__":
	count(sys.stdin)


