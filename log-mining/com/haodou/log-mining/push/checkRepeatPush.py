import sys

lastU=""
ts={}
for line in sys.stdin:
	cols=line.strip().split("\t")
	u=cols[0]
	if lastU=="":
		lastU=u
	if lastU != u:
		lastU=u
		ts={}
	tag=cols[1]
	if tag in ts:
		raise Exception("repeat tag("+tag+") for user("+u+")\n")
	ts[tag]=1


