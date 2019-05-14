import sys

us={}
for line in open("user.txt"):
	cols=line.strip().split("\t")
	if len(cols) < 1:
		continue
	u=cols[0]
	us[u]=1

for line in sys.stdin:
	cols=line.strip().split("\t")
	if cols[1] == "":
		continue
	u=cols[0]
	if u not in us:
		continue
	print cols[0]+"\t"+cols[1]


