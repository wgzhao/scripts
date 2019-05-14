import sys

sys.path.append("./")
sys.path.append("../")

from column import *

nums={}
for line in sys.stdin:
	cols=line.split("\t")
	if len(cols) < APP_LOG_COLUMNS:
		continue
	method=cols[METHOD_CID]
	if method in nums:
		nums[method]+=1
	else:
		nums[method]=1
	m=readPara(cols,"recipe.getcollectrecomment","type")
	if m in nums:
		nums[m]+=1
	else:
		nums[m]=1

for method in nums:
	print method+"\t"+str(nums[method])

