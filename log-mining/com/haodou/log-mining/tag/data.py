#encoding=utf-8

import sys
sys.path.append("./")
sys.path.append("../util")

import DBUtil2

def step():
	db=DBUtil2.DB()
	db.connect("haodou_recipe")
	cursor=db.execute("select RecipeId,StepOrder,Intro from RecipeStep;")
	ret=cursor.fetchall()
	for r in ret:
		if r[2] == None:
			"%d\t%d\tn/a"%(r[0],r[1])
		else:
			print "%d\t%d\t%s"%(r[0],r[1],r[2].encode("utf-8"))

if __name__=="__main__":
	step()


