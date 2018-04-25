import MySQLdb

class DB:  
  conn = None  
  cursor = None  
  def connect(self):  
  	self.conn = MySQLdb.connect (host='10.1.1.70', user='bi',passwd='bi_haodou',db='haodou_recipe',charset="utf8") 
  	
  def execute(self, sql):  
    try:  
      cursor = self.conn.cursor()  
      cursor.execute(sql)  
    except (AttributeError, MySQLdb.OperationalError):  
      self.connect()  
      cursor = self.conn.cursor()  
      cursor.execute(sql)  
    return cursor  
  
  def close(self):  
    if(self.cursor):  
      self.cursor.close()  
    self.conn.commit()  
    self.conn.close()  

def test():
	db=DB()
	cursor=db.execute("select recipeid,Title from Recipe;")
	ret=cursor.fetchall()
	for r in ret:
		if r[1] != None and len(r[1]) > 0:
			print str(r[0])+"\t"+r[1].encode("utf-8")
	db.close()

if __name__=="__main__":
	test()

