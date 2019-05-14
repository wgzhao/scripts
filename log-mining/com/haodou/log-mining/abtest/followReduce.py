import sys

sys.path.append("./")
sys.path.append("../log/")

import dictInfo

def outputa(lastMethod,paras,paraUs):
	allu={}
	for p in paras:
		print lastMethod+"-"+p+"\t"+str(dictInfo.info(paraUs[p]))
		for u in paraUs[p]:
			if u not in allu:
				allu[u]=paraUs[p][u]
			else:
				allu[u]+=paraUs[p][u]
		paraUs[p]={}
	print lastMethod+"\t"+str(dictInfo.info(allu))

def output(lastMethod,paras,paraUs,paraQs):
	outputa(lastMethod,paras,paraUs)
	outputa(lastMethod+"_request",paras,paraQs)

def reduce(f):
	lastMethod=""
	paras={}
	paraUs={}
	paraQs={}
	for line in f:
		cols=line.strip().split("\t")
		if len(cols) < 4:
			sys.stderr.write(line)
			continue
		method=cols[0]
		if lastMethod == "":
			lastMethod=method
		if lastMethod != method:
			output(lastMethod,paras,paraUs,paraQs)
			lastMethod=method
			paras={}
			paraUs={}
			paraQs={}
		p=cols[1]
		if p not in paras:
			paras[p]=0
			paraUs[p]={}
			paraQs[p]={}
		u=cols[2]
		if u not in paraUs[p]:
			paraUs[p][u]=0
		q=cols[3]
		if q not in paraQs[p]:
			paraQs[p][q]=0
		if len(cols) >= 5:
			paras[p]+=1
			paraUs[p][u]+=1
			paraQs[p][q]+=1
	if lastMethod != "":
		output(lastMethod,paras,paraUs,paraQs)

if __name__=="__main__":
	reduce(sys.stdin)

