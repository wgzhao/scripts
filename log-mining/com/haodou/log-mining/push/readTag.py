#encoding=utf-8

import sys

def read(type):
	ds={}
	for line in open("mergeCateName.txt"):
		cols=line.strip().split()
		w=cols[0]
		if w not in ds:
			ds[w]={}
		for t in cols[1:]:
			ds[w][t]=1
	for w in ds:
		s=[]
		for t in ds[w]:
			s.append(t)
		while len(s) > 0:
			if s[0] in ds:
				for tt in ds[s[0]]:
					if tt not in ds[w]:
						ds[w][tt]=1
						s.append(tt)

			del s[0]
	ts={}
	ret={}
	for w in ds:
		if type in ds[w]:
			ts[w]=1
			ret[w]=1
	#for t in ts:
		#print "tt",t
	#ret={}
	for w in ds:
		for t in ds[w]:
			if t in ts:
				#print "t",t
				ret[w]=1
	s=""
	for w in ret:
		s+=","+w
	return s

if __name__=="__main__":
	if len(sys.argv) >= 2:
		s=""
		for type in sys.argv[1:]:
			s+=read(type)
		print s
	else:
		read("[食疗]")


