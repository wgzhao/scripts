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

def ABRid(line,key,vs=None):
	line=line.strip()
	if len(line) == 0:
		return
	(v,endPos)=column2.getValue(line,'"'+key+'":"','"')
	if v == None:
		return
	(qid,endPos)=column2.getValue(line,'"request_id":"','"')
	if qid == None:
		return
	print qid+"\t"+v

if __name__=="__main__":
	if len(sys.argv) < 2:
		for line in sys.stdin:
			ABRid(line,"_AB")
	else:
		for line in sys.stdin:
			ABRid(line,sys.argv[1])

