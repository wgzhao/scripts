#encoding=utf-8

import sys

us={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	u=cols[0]
	if u in us:
		sys.stderr.write(u+"\n")
		continue
	print line.strip()
	us[u]=1


