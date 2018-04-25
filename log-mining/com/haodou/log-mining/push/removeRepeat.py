#encoding=utf-8

import sys

def remove(lastFile):
	us={}
	for line in open(lastFile):
		cols=line.strip().split("\t")
		us[cols[0]]=1
	for line in sys.stdin:
		cols=line.strip().split("\t")
		if cols[0] in us:
			continue
		print line.strip()

if __name__=="__main__":
	remove(sys.argv[1])

