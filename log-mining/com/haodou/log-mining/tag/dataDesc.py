#encoding=utf-8

class Data(object):
	def __init__(self):
		self.location=""
		self.name=""

	#标准的格式是行列格式，utf-8
	def getFormat(self):
		if self.format == None:
			return "columns"


	def setColumnType(cols):
		self.cols=cols
	
	def addColumnRela(i,j,rel):
		if i not in self.rels:
			self.rels[i]={}
		if j not in self.rels[i]:
			self.rels[i][j]=[]
		self.rels[i][j].append(rel)

class DataBank(object):
	def __init__(self):
		pass


