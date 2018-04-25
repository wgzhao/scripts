#encoding=utf-8

#示例调用：hdfs dfs -text /user/yarn/logs/source-log.http.m_haodou_com/logdate=2015-03-04/* |  grep 184522 | python topicCount.py 
#

import sys

ips={}
hips={}
v1=0
v2=0
v3=0
v4=0
ts={}
for line in sys.stdin:
	cols=line.strip().split('\01')
	if len(cols) < 6:
		continue
	ip=cols[0]
	ips[ip]=1
	url=cols[4]
	if url.find("topic") >= 0:
		if url.find("view") >= 0:
			v1+=1
		elif url.find("html") >= 0:
			p=url.find("topic-")
			p1=url.find(".html",p)
			t=url[p+len("topic-"):p1]
			if t not in ts:
				ts[t]=1
			else:
				ts[t]+=1
			v2+=1
			hips[ip]=1
		else:
			v3+=1
	else:
		v4+=1
print len(ips),len(hips)
print v1,v2,v3
print ts
