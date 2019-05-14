import sys

sys.path.append("./")
sys.path.append("../")
sys.path.append("../log")

import dictInfo

def outputAB(lastId,paraUs,ABList,allParaUs):
	if ABList == None:
		return
	sum={}
	for para in paraUs:
		print para
		sum[para]=0
		if para not in allParaUs:
			allParaUs[para]={}
		us=paraUs[para]
		aus=allParaUs[para]
		for u in us:
			if u not in aus:
				aus[u]=0
			for rid in us[u]:
				if rid != 0 and rid not in ABList:
					sys.stderr.write(lastId+"\t"+str(rid)+"\t"+str(ABList)+"\n")
					us[u][rid]=0
				aus[u]+=us[u][rid]
				sum[para]+=us[u][rid]
	return sum

def output(lastId,paraUs,ABList,AB,AParaUs,BParaUs,AParaQs,BParaQs):
	if ABList == None:
		return
	if AB=="A":
		sum=outputAB(lastId,paraUs,ABList,AParaUs)
		for para in sum:
			qpara=para+"_request"
			if qpara not in AParaQs:
				AParaQs[qpara]={}
			AParaQs[qpara][lastId]=sum[para]
	else:
		sum=outputAB(lastId,paraUs,ABList,BParaUs)
		for para in sum:
			qpara=para+"_request"
			if qpara not in BParaQs:
				BParaQs[qpara]={}
			BParaQs[qpara][lastId]=sum[para]
		
	
def printParas(AB,paraUs):
	sum=0.0
	unum=0.0
	for para in paraUs:
		info=dictInfo.staticInfo(paraUs[para])
		sum+=info["sum"]
		unum+=info["userNum"]
		print AB+"_"+para+"\t"+str(info)
	print AB+"\t"+str(sum)+"\t"+str(unum)+"\t"+str(sum/(unum+1e-16))

def count(f):
	lastId=""
	paraUs={}
	AParaUs={}
	BParaUs={}
	AParaQs={}
	BParaQs={}
	ABList=None
	AB=""
	for line in f:
		cols=line.strip().split("\t")
		id=cols[0]
		if lastId == "":
			lastId=id
		if lastId != id:
			output(lastId,paraUs,ABList,AB,AParaUs,BParaUs,AParaQs,BParaQs)
			lastId=id
			paraUs={}
			ABList=None
			AB=""
		if cols[1] == "A" or cols[1] == "B":
			AB=cols[1]
			ABList=eval(cols[2])
		else:
			u=cols[1]
			para=cols[2]
			if para not in paraUs:
				paraUs[para]={}
			if u not in paraUs[para]:
				paraUs[para][u]={}
				paraUs[para][u][0]=0
			if len(cols) >= 4:
				paraUs[para][u][int(cols[3])]=1

	if lastId != "":
		output(lastId,paraUs,ABList,AB,AParaUs,BParaUs,AParaQs,BParaQs)
		printParas("A",AParaUs)
		printParas("B",BParaUs)
		printParas("QA",AParaQs)
		printParas("QB",BParaQs)

if __name__=="__main__":
	count(sys.stdin)

