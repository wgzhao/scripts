#encoding=utf-8

import sys

idReserveForRecipe=3000000

#输入1： tag r n v
#输入2：tag tid
#
#对于输入1，只截取tag
#对于输入2，原样输出
def listTags():
	lastA=""
	for line in sys.stdin:
		cols=line.strip().split("\t")
		A=cols[0]
		if lastA != A:
			if len(cols) > 2:
				print A
			else:
				print line.strip()
			lastA=A
import re
ridp=re.compile("^rid-[0-9]{1,7}$")

def ridTag(tag):
	if ridp.match(tag):
		return int(tag[4:])
	return -1

def tag2idDict():
	newTags={}
	max=idReserveForRecipe
	lastTag=""
	has=False
	for line in sys.stdin:
		cols=line.strip().split("\t")
		tag=cols[0]
		if lastTag == "":
			lastTag=tag
		if lastTag != tag:
			if not has:
				newTags[cols[0]]=1
			lastTag=tag
			has=False
		if len(cols) == 2:
			tid=int(cols[1])
			if tid > max:
				max=tid
			print line.strip()
			has=True
	if lastTag != "":
		if not has:
			newTags[cols[0]]=1
	for tag in newTags:
		if ridTag(tag) >= 0:
			print tag+"\t"+str(ridTag(tag))
			continue
		max+=1
		print tag+"\t"+str(max)

def dataOutput(lines,id):
	if len(id) == 0:
		return
	for c in lines:
		c[0]=id
		print "\t".join(c)

def tag2idData():
	lastA=""
	lines=[]
	id=""
	for line in sys.stdin:
		cols=line.strip().split("\t")
		A=cols[0]
		if lastA == "":
			lastA=A
		if lastA != A:
			dataOutput(lines,id)
			lastA=A
			lines=[]
			id=""
		if len(cols) > 2:
			lines.append(cols)
		else:
			id=cols[1]
	if lastA != "":
		dataOutput(lines,id)

def exchange():
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if len(cols) > 2:
			tmp=cols[0]
			cols[0]=cols[1]
			cols[1]=tmp
			print "\t".join(cols)
		else:
			print line.strip()

if __name__=="__main__":
	if len(sys.argv) >= 2:
		t=sys.argv[1]
		if t == "listTags":
			listTags()
		elif t == "dict":
			tag2idDict()
		elif t == "data":
			tag2idData()
		elif t == "exchange":
			exchange()


