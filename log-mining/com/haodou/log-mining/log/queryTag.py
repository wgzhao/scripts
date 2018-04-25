#encoding=utf-8

import codecs
import sys

def inner(p,s):
	if s > 0 and s < len(p):
		return True
	return False

def isNumY(c):
	if (c <= '9' and c >= '0') or (c <= 'z' and c >= 'a') or (c <= 'Z' and c >= 'A'):
		return True
	return False

def splitNumY(p,s):
	if inner(p,s) and isNumY(p[s-1]) and isNumY(p[s]):
		return True
	return False

def readWords(file):
	d = {}
	for line in codecs.open(file,"r","utf-8"):
		cols=line.strip().split("\t")
		if len(cols) == 0:
			continue
		if len(cols) >= 2:
			if cols[1].isdigit():
				d[cols[0]]=int(cols[1])
			else:
				d[cols[0]]=cols[1]
		else:
			d[cols[0]]=1
	return d

import re
NumP=re.compile(r"^[0-9]+$")
NumYP=re.compile(r"^[0-9a-zA-Z]+$")

def addTag(tags,sub,tag):
	if sub not in tags:
		tags[sub]={}
	tags[sub][tag]=1

def readCate(file):
	n2ps={}
	for line in codecs.open(file,"r","utf-8"):
		line=line.strip()
		if len(line) == 0:
			continue
		cols=line.split("\t")
		if len(cols) < 2:
			continue
		word=cols[0]
		if len(word) == 0:
			continue
		for i in range(1,len(cols),1):
			p=cols[i]
			if len(p) ==0:
				continue
			if word not in n2ps:
				n2ps[word]={}
			if p not in n2ps[word]:
				n2ps[word][p]=1
	return n2ps

def getAll(es,t,tagm):
	if t in tagm:
		for pt in tagm[t]:
			if pt in es:
				continue
			es[pt]=1
			getAll(es,pt,tagm)

TAG_FIX = "["
TAG_FIX_END = "]"

def tagMap(n2ps):
	tagm={}
	es={}
	add=1
	while add > 0:
		add=0
		for tag in n2ps:
			if tag[0] != TAG_FIX or tag[-1]!=TAG_FIX_END:
				continue
			mlist=[]
			es={}
			for t in n2ps[tag]:
				es[t]=1
				getAll(es,t,tagm)
			size=0
			if tag in tagm:
				oldlist=tagm[tag]
				size=len(oldlist)
				for w in oldlist:
					es[w]=1
			for e in es:
				mlist.append(e)
			if len(mlist) > size:
				add += (len(mlist)-size)
				tagm[tag]=mlist
	return tagm


MAXN = 10
CONTEXT_FIX = "{"
CONTEXT_FIX_END = "}"
NOT_FIX = "^"
OUT_FIX = "$"
VOID_MODE=CONTEXT_FIX+NOT_FIX+OUT_FIX+CONTEXT_FIX_END
numTag="[num]"
engTag="[english]"
UNKOWN_TAG="[unkown]"
nd={}
partCates={}
dict={}

partCates={}
tmp=readWords("partCates.txt")
for tt in tmp:
	partCates[u"["+tt+u"]"]=1

segDict=readWords("segDict.txt")

nd=readCate("mergeCateName.txt")

tagm=tagMap(nd)

needWords=readWords("needWords.txt")

def match(phrase,tags,s,e,isWhole):
	#print "phrase:",phrase[s:e].encode("utf-8")
	ret=0
	i=e
	while i > s:
		if splitNumY(phrase,i):
			i-=1
			continue
		MN=MAXN
		while splitNumY(phrase,i-MN):
			MN+=1
		rightMode=VOID_MODE
		if i < len(phrase):
			rightMode=CONTEXT_FIX+NOT_FIX+phrase[i]+CONTEXT_FIX_END
		j=MN
		if i - j < s:
			j = i-s
		has=False
		while j > 0:
			if not isWhole and j >= (e-s):
				j-=1
				continue
			w=phrase[i-j:i]
			#print w.encode("utf-8")
			sub=(i-j,w)
			#print i,j,len(w),w.encode("utf-8")
			if isNumY(w[0]):
				if NumP.match(w):
					addTag(tags,sub,numTag)
					has=True
				elif NumYP.match(w):
					addTag(tags,sub,engTag)
					has=True
			#print "w:",w.encode("utf-8")
			if w in nd:
				nlist=None
				if w + rightMode in nd:
					nlist=nd[w+rightMode]
				else:
					leftMode=VOID_MODE
					if i -j - 1 >= 0:
						leftMode=CONTEXT_FIX+NOT_FIX+phrase[i-j-1]+CONTEXT_FIX_END
					if leftMode+w in nd:
						nlist=nd[leftMode+w]
				part=False
				for t in nd[w]:
					if nlist != None and t in nlist:
						continue
					#print "t:",t.encode("utf-8")
					if t in tagm:
						for pt in tagm[t]:
							#print "pt:",pt.encode("utf-8")
							if pt in partCates:
								part=True
					if t in partCates:
						part=True
					has=True
					addTag(tags,sub,t)
				if part:
					match(phrase,tags,i-j,i,False)
			if not has and w in segDict:
				addTag(tags,sub,UNKOWN_TAG)
				has=True
			if has:
				i-=j
				break
			j-=1
		if not has:
			i-=1

sys.path.append(".")
sys.path.append("..")
import column

def getAllTags(word):
	word=word.decode("utf-8")
	allTags={}
	tags={}
	match(word,tags,0,len(word),True)
	#print tags
	for sub in tags:
		allTags[sub[1].encode("utf-8")]=1
		for tag in tags[sub]:
			allTags[tag.encode("utf-8")]=1
			if tag not in tagm:
				continue
			for t in tagm[tag]:
				allTags[t.encode("utf-8")]=1
	return allTags

def testMatch():
	for line in sys.stdin:
		tags={}
		word=line.strip().decode("utf-8")
		match(word,tags,0,len(word),True)
		for sub in tags:
			print sub[1].encode("utf-8")
			for tag in tags[sub]:
				print tag.encode("utf-8")
				if tag not in tagm:
					continue
				for t in tagm[tag]:
					print t.encode("utf-8")

def testGetAllTags():
	for line in sys.stdin:
		word=line.strip()
		if len(word) == 0:
			continue
		print word
		allTags=getAllTags(word)
		for t in allTags:
			print "\t",t

def testTag():
	import column
	for line in sys.stdin:
		line=line.strip()
		cols=line.split("\t")
		if len(cols) < column.APP_LOG_COLUMNS+1:
			continue
		method=cols[column.METHOD_CID]
		if not method.startswith("search.getlist"):
			continue
		keyword=column.getValue(cols[column.PARA_ID],"keyword")
		if keyword == None or keyword == "":
			continue
		tags={}
		kw=keyword
		#sys.stderr.write(keyword+"\n")
		try:
			keyword=keyword.decode("utf-8")
		except UnicodeDecodeError:
			sys.stderr.write(keyword+"\n")
			continue
		offset=column.getValue(cols[column.PARA_ID],"offset")
		if offset != "" and offset != "0":
			continue
		match(keyword,tags,0,len(keyword),True)
		uuid=column.uuidFirst(cols)
		if uuid == None or uuid == "":
			continue
		d={}
		for sub in tags:
			d[sub[1]]=1
			for tag in tags[sub]:
				d[tag]=1
				if tag not in tagm:
					continue
				for t in tagm[tag]:
					d[t]=1
		s=uuid+"\t"+kw
		for t in d:
			s+="\t"+t.encode("utf-8")
		print s
			
def testSplitNumY():
	s="的23cd3r人民4"
	for i in range(len(s)):
		print splitNumY(s,i),s[i-1:i+1]

if __name__=="__main__":
	#testSplitNumY()
	if len(sys.argv) > 1:
		testMatch()
	else:
		testTag()

