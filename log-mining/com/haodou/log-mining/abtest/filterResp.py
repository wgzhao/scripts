import sys

def listRid(line):
	line=line.strip()
	if len(line) == 0:
		return
	if line.find("ABTESTING_HOT_RECIPE") < 0:
		return
	try:
		while line.find("false") > 0:
			line=line.replace("false","False")
		a=eval(line.strip())
	except ValueError:
		return
	if "_AB" not in a:
		return
	if a["_AB"] == "ABTESTING_HOT_RECIPE:B":
		ABOption="B"
	elif a["_AB"] == "ABTESTING_HOT_RECIPE:A":
		ABOption="A"
	else:
		return
	if "list" not in a["result"] or len(a["result"]["list"]) <= 0 or "RecipeId" not in a["result"]["list"][0]:
		return
	rids=[]
	for t in a["result"]["list"]:
		rids.append(t["RecipeId"])
	print a["request_id"]+"\t"+ABOption+"\t"+str(rids)

sys.path.append("./")
import column2

abts={}

def ABRid(line,key,vs=None):
	line=line.strip()
	if len(line) == 0:
		return
	(v,endPos)=column2.getValue(line,'"'+key+'":"','"')
	if v == None:
		return
	v=column2.escapeUnicode(v).encode("utf-8")
	abts[v]=1
	vv=v.split("|")
	if vs != None:
		has=False
		for w in vv:
			if w in vs:
				has=True
		if not has:
			return
	(qid,endPos)=column2.getValue(line,'"request_id":"','"')
	if qid == None:
		return
	print qid+"\t"+v
	

if __name__=="__main__":
	vs={}
	for line in open("ABOption.txt"):
		vs[line.strip()]=1
	if len(vs) == 0:
		vs=None
	if len(sys.argv) < 2:
		for line in sys.stdin:
			ABRid(line,"_AB",vs)
	else:
		for line in sys.stdin:
			ABRid(line,sys.argv[1],vs)
	for w in abts:
		sys.stderr.write(w+"\n")
