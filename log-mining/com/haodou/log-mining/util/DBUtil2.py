import MySQLdb

class DB:  
  conn = None  
  cursor = None
  db = None
  def connect(self,db):  
  	#self.conn = MySQLdb.connect (host='10.1.1.8', port=33060,user='bi',passwd='bi_haodou',db=db,charset="utf8") 
	self.conn = MySQLdb.connect (host='10.0.10.85',port=3306,user='bi',passwd='bi_haodou',db=db,charset="utf8")
	self.db=db

  def execute(self, sql):  
    try:  
      cursor = self.conn.cursor()  
      cursor.execute(sql)  
    except (AttributeError, MySQLdb.OperationalError):  
      self.connect(self.db)  
      cursor = self.conn.cursor()  
      cursor.execute(sql)  
    return cursor  
  
  def close(self):  
    if(self.cursor):  
      self.cursor.close()  
    self.conn.commit()  
    self.conn.close()  

def strR(r):
	s=""
	for rc in r:
		if type(rc) == unicode:
			rcs=rc.encode("utf-8")
		else:
			rcs=str(rc)
			if rcs.endswith("L"):
				rcs=rcs[0:-1]
		if len(s) > 0:
			s+="\t"
		s+=rcs
	return s

def queryPrint(dbName,query,strFunc=strR):
	db=DB()
	db.connect(dbName)
	cursor=db.execute(query)
	ret=cursor.fetchall()
	for r in ret:
		print strFunc(r)


def queryPrintLarge(dbName,query,N=500000,strFunc=strR,prefix=""):
	db=DB()
	db.connect(dbName)
	start=0
	size=N
	while size >= N: 
		cursor=db.execute(query+" limit "+str(start)+","+str(N))
		ret=cursor.fetchall()
		for r in ret:
			print prefix+strFunc(r)
		start+=len(ret)
		size=len(ret)


def test():
	ids={}
	for line in open("./dunguo.txt"):
		ids[line.strip()]=1
	db=DB()
	#db.connect("haodou_center")
	db.connect("haodou_comment")
	cursor=db.execute("select itemid,content from Comment where type=0 and status = 1;")
	ret=cursor.fetchall()
	for r in ret:
		if str(r[0]) in ids:
			print "%d\t%s"%(r[0],r[1].encode("utf-8"))
	db.close()

def getTitle():
	ids={}
	for line in open("./dunguo.txt"):
		ids[line.strip()]=1
	ts={}
	db=DB()
	db.connect("haodou_recipe")
	cursor=db.execute("select recipeid,title from Recipe where status=0;")
	ret=cursor.fetchall()
	for r in ret:
		if str(r[0]) in ids:
			t=r[1].encode("utf-8")
			if t not in ts:
				ts[t]=1
			else:
				ts[t]+=1
			#print "%d\t%s"%(r[0],r[1].encode("utf-8"))
	for t in ts:
		print "%s\t%d"%(t,ts[t])
	db.close()

def UserSuggest():
	#db.connect("haodou_admin")
	#cursor=db.execute("select * from haodou_admin")
	dbName="haodou_admin"
	query="select * from UserSuggest limit 10"
	queryPrint(dbName,query)

if __name__=="__main__":
	#test()
	#getTitle()
	UserSuggest()

