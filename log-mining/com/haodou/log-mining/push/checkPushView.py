#encoding=utf-8
#
#检查app log或者m.haodou.com的log与push view的记录是否一致
#

#调用实例：hdfs dfs -text /online_logs/beijing/behaviour/pushview/2015-03-15/* /user/yarn/logs/source-log.http.m_haodou_com/logdate=2015-03-15/* | python checkPushView.py

import sys
sys.path.append("../")
import column

def topicCheck(id,title):
	idType="album"
	if id.startswith("topic"):
		idType="topic"
	cs={}
	cvs={}
	cs["Android"]={}
	cs["Appstore"]={}
	cs["other"]={}
	pvs={}
	pvs["Android"]={}
	pvs["Appstore"]={}
	pvs["other"]={}
	us={}
	for line in sys.stdin:
		if idType == "topic" and line.find("m.haodou.com") > 0:
			cols=line.strip().split("\01")
			us[cols[0]]=1
			if not line.find(id+".") > 0:
				continue
			type="other"
			if line.find("Android") > 0:
				type="Android"
			elif line.find("iPhone") >0:
				type="Appstore"
			ip=cols[0]
			cs[type][ip]=1
		elif line.find(title) > 0:
			#print line
			type="other"
			if line.find('"appid":"2"') > 0:
				type="Android"
			elif line.find('"appid":"4"') > 0:
				type="Appstore"
			if idType == "album":
				s=line.find('"uuid":"')
				if s < 0:
					continue
				e=line.find('"',s+len('"uuid":"')+1)
				if e < 0:
					continue
				u=line[s+len('"uuid":"'):e]
			else:
				s=line.find('"user_ip":"')
				if s < 0:
					continue
				e=line.find('"',s+len('"user_ip":"')+1)
				if e < 0:
					continue
				u=line[s+len('"user_ip":"'):e]
			s=line.find('"vn":"')
			if s < 0:
				version=""
			else:
				e=line.find('"',s+len('"vn":"')+1)
				if e < 0:
					version=""
				else:
					version=line[s+len('"vn":"'):e]
			if version.startswith("v"):
				version=version[1:]
			if version not in pvs[type]:
				pvs[type][version]={}
			pvs[type][version][u]=1
		else:
			cols=line.strip().split("\t")
			if len(cols) < column.APP_LOG_COLUMNS:
				continue
			uuid=column.uuid(cols)
			us[uuid]=1
			method=cols[column.METHOD_CID]
			version=cols[column.VERSION_CID]
			if version.startswith("v"):
				version=version[1:]
			if method != "info.getalbuminfo":
				continue
			aid=column.getValue(cols[column.PARA_ID],"aid")
			if aid != id:
				continue
			appid=cols[column.APPID_CID]
			type="other"
			if appid == "2":
				type="Android"
			elif appid == "4":
				type="Appstore"
			if uuid == None or uuid == "":
				continue
			cs[type][uuid]=1
			if type not in cvs:
				cvs[type]={}
			if version not in cvs[type]:
				cvs[type][version]={}
			cvs[type][version][uuid]=1
	#print cs
	for type in pvs:
		for version in pvs[type]:
			viewNum=len(pvs[type][version])
			innNum=0
			notNum=0
			for u in pvs[type][version]:
				if u in cs[type]:
					#print "In\t%s\t%s\t%s"%(type,u,version)
					innNum+=1
				elif u not in us:
					#print "Not\t%s\t%s\t%s"%(type,u,version)
					notNum+=1
			print "%s\t%s\t%d\t%d\t%.4f\t%d\t%.4f"%(type,version,viewNum,notNum,notNum/(viewNum+1e-12),innNum,innNum/(viewNum+1e-12))
	for type in cvs:
		for version in cvs[type]:
			viewNum=len(cvs[type][version])
			notNum=0
			for uuid in cvs[type][version]:
				if type not in pvs or version not in pvs[type] or uuid not in pvs[type][version]:
					notNum+=1
			innNum=viewNum-notNum
			print "appLog:%s\t%s\t%d\t%d\t%.4f\t%d\t%.4f"%(type,version,viewNum,notNum,notNum/(viewNum+1e-12),innNum,innNum/(viewNum+1e-12))

if __name__=="__main__":
	#3.15
	#topicCheck("topic-333928","谣言粉碎机来了")
	#3.7
	#topicCheck("topic-332855","\u5973\u4eba\u597d\u547d\u9910\u5355\u5927\u5949\u732e\uff0c\u4f60\u770b\u8d77\u6765\u90fd\u597d\u597d\u5403\u5594\ufe4c\u25cb\ufe4b")
	topicCheck(sys.argv[1],sys.argv[2])


