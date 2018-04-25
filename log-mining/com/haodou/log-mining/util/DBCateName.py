#encoding=utf-8

import sys


def readCates():
	import DBUtil2
	db=DBUtil2.DB()
	db.connect("haodou_recipe")
	cursor=db.execute("select CateNameId,Name,ParentId from CateName where UserId = 0 and ParentId != 0;")
	ret=cursor.fetchall()
	cateidName={}
	for r in ret:
		cateidName[r[0]]=(r[1].encode("utf-8"),r[2])
	return cateidName

def readCateFile(f):
	cates=[""]
	for line in f:
		cols=line.strip().split("\t")
		try:
			id=int(cols[0])
			while id > len(cates):
				cates.append("")
			cates.append(cols[1])
		except:
			#sys.stderr.write(line)
			pass
	return cates

if __name__ == "__main__":
	cateidName=readCates()
	ps={}
	for id in cateidName:
		p=cateidName[id][1]
		ps[p]=1
		#print str(id)+"\t"+cateidName[id][0]+"\t"+str(cateidName[id][1])
	for id in cateidName:
		if id not in ps:
			print str(id)+"\t"+cateidName[id][0]+"\t"+str(cateidName[id][1])
	#cates=readCateFile(open("cateidName.txt"))
	#print len(cates)


