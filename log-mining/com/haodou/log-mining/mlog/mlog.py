import sys

sys.path.append("./")
sys.path.append("../")

def addOne(prefix,m,u,nums):
	if m == None or len(m) == 0:
		return
	print prefix+"\t"+u+"\t"+m
	if m in nums:
		nums[m]+=1
	else:
		nums[m]=1

def value(line,key):
	pos=line.find(key)
	if pos < 0:
		return None
	pos=pos+len(key)
	e=line.find('"',pos)
	e0=line.find(',',pos)
	if e0 > 0 and (e < 0 or e > e0):
		e=e0
	e1=line.find('&',pos)
	if e < 0:
		return None
	if e1 > 0 and e1 < e:
		e=e1
	return line[pos:e]

def getU(line):
	slen=len('{"remote_addr":"')
	if line.startswith('{"remote_addr":"'):
		pos=line.find('"',slen)
		if pos <0:
			return None
		return line[slen:pos]
	return None

import re
TopicIdP=re.compile("[1-9][0-9]*")

def getTopic(line):
	path=value(line,'"path":"')
	if path != None and path != "":
		pos=path.find("/topic/view.php?id=")
		if pos > 0:
			v=path[pos+len("/topic/view.php?id="):]
			if TopicIdP.match(v):
				v=TopicIdP.match(v).group(0)
				return v
	return None

def count():
	nums={}
	for line in sys.stdin:
		u=getU(line)
		if u == None:
			continue
		topic=getTopic(line)
		if topic != None:
			addOne("topic",topic,u,nums)
		else:
			if line.find('learn.php') > 0:
				addOne('learn.php',u,u,nums)

if __name__=="__main__":
	count()

