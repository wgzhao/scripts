#encoding=utf-8

import sys

sys.path.append("./")

#
#过滤非法条目：包括非法用户，非法菜谱，非法话题，非法成果照
#后续每增加一个条目都要增加
#

ActionMinColumns=5

#
#行为统计数据第1列和第3列交换，以便将第2列作为key
#如果第3列是非法条目，要将该行记录去掉。
#
#input1: item1 type item2 [v]  行为统计数据，最少5列
#input2: item        非法条目列表,最多两列
#
def merge():
	for line in sys.stdin:
		cols = line.strip().split("\t")
		if len(cols) >= ActionMinColumns:
			tmp=cols[0]
			cols[0]=cols[2]
			cols[2]=tmp
			print "\t".join(cols)
		else:
			print line.strip()

def output(lastKey,isValid,lines):
	if isValid:
		for line in lines:
			print line.strip()
	else:
		for line in lines:
			sys.stderr.write(line+"\n")

def reduce():
	lastKey=""
	isValid=True
	lines=[]
	for line in sys.stdin:
		cols=line.strip().split("\t")
		key=cols[0]
		if lastKey == "":
			lastKey=key
		if lastKey != key:
			output(lastKey,isValid,lines)
			lastKey=key
			lines=[]
			isValid=True
		if len(cols) >= ActionMinColumns:
			tmp=cols[0]
			cols[0]=cols[2]
			cols[2]=tmp
			lines.append("\t".join(cols))
		else:
			isValid=False

	if lastKey != "":
		output(lastKey,isValid,lines)


if __name__=="__main__":
	if sys.argv[1] == "map":
		merge()
	elif sys.argv[1] == "reduce":
		reduce()


