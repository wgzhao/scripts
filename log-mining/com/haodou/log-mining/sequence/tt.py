#encoding=utf-8

import sys

def read(f):
	d={}
	for line in open(f):
		d[line.strip()]=1
	return d

def diff(f1,f2):
	d1=read(f1)
	d2=read(f2)
	for w in d1:
		if w not in d2:
			print w,"d1"
	for w in d2:
		if w not in d1:
			print w,"d2"

if __name__=="__main__":
	diff(sys.argv[1],sys.argv[2])

