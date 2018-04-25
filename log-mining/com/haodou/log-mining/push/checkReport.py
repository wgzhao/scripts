#encoding=utf-8
#安卓端延迟上报，可能会漏报
#因此，检测push当天专辑点击数目与上报的专辑点击数目的差别
#分安卓和IOS端
#

import sys

sys.path.append("../")
import column

def appLogHit(aid):
	channels={}
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if not column.valid(cols):
			continue
		method=cols[column.METHOD_CID]
		if method != "info.getalbuminfo":
			continue
		version=column.intVersion(cols[column.VERSION_CID])
		if version < 400:
			continue
		taid=column.getValue(cols[column.PARA_ID],"aid")
		if taid != aid:
			continue
		appid=cols[column.APPID_CID]
		if appid not in channels:
			channels[appid]=1
		else:
			channels[appid]+=1
	for appid in channels:
		print "%s\t%d"%(appid,channels[appid])

if __name__=="__main__":
	appLogHit(sys.argv[1])

