import sys

sys.path.append("../util")
import DictUtil

SmoothWeit=0.8

def readWeit():
	ws={}
	for line in open("needWords.txt"):
		if line.startswith("#"):
			continue
		cols=line.strip().split("\t")
		w=cols[0]
		try:
			v=float(cols[3])-SmoothWeit
		except:
			continue
		ws[w]=v
	return ws

tagWeit=readWeit()

#for w in tagWeit:
#	sys.stderr.write("tagweit:%s\t%.3f\n"%(w,tagWeit[w]))


def cosineWeit(d1,d2):
	t1={}
	for w in d1:
		if w in tagWeit:
			t1[w]=d1[w]*tagWeit[w]
	t2={}
	for w in d2:
		if w in tagWeit:
			t2[w]=d2[w]*tagWeit[w]
	return DictUtil.cosine(t1,t2)




