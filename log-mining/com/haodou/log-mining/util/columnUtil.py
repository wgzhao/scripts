#encoding=utf-8

import sys

sys.path.append("./")
sys.path.append("../")

def escapeUnicode(v):
	if v.find("\u") >= 0:
		while v.endswith("\\"):
			v=v[0:-1]
		v=eval('u'+'"'+v+'"')
	return v

def escapeUtf8(v):
	if v.find("\u") >= 0:
		while v.endswith("\\"):
			v=v[0:-1]
		v=eval('u'+'"'+v+'"').encode("utf-8")
	return v

def getValue(para,before,after,begin=0):
	pos=para.find(before,begin)
	#print para[begin:begin+100],before,pos
	if pos < 0:
		return (None,begin)
	pos1=para.find(after,pos+len(before))
	if pos1 > pos:
		v=para[pos+len(before):pos1]
		return (v,pos1)
	return (None,pos)

def getStrValue(para,name):
	name='"'+name+'":"'
	pos=para.find(name)
	if pos < 0:
		return None
	pos+=len(name)
	e=para.find('"',pos)
	if e < 0:
		return None
	return para[pos:e]

def getIntValue(para,name):
	name='"'+name+'":'
	pos=para.find(name)
	if pos < 0:
		return None
	pos+=len(name)
	e=para.find(',',pos)
	if e < 0:
		return None
	return para[pos:e]

def parseUrl(url):
	if url.find("topic") < 0:
		return url
	s=url.find("?id=")
	if s < 0:
		return url
	e=url.find("&")
	if e < 0:
		e=len(url)
	return url[s+len("?id="):e]

def test():
	for line in open("/home/zhangzhonghui/data/push/2014-12-02/pushview.txt"):
		print line
		print "uuid",getStrValue(line,"uuid")
		break

if __name__=="__main__":
	test()
	print parseUrl('"Url": "http://m.haodou.com/app/recipe/topic/view.php?id=320435"')
