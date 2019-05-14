import MySQLdb

class DB:
  conn = None
  cursor = None
  def connect(self):
    #self.conn = MySQLdb.connect (host='192.168.1.118', user='bi',passwd='bi_haodou',db='haodou_server',charset="utf8")
  	#self.conn = MySQLdb.connect (host='10.0.11.140', user='bi',passwd='bi_haodou!@#',db='dw',charset="utf8")
  	self.conn = MySQLdb.connect (host='211.151.138.246', user='bi',passwd='bi_haodou!@#',db='dw',charset="utf8")

  def execute(self, sql):
    try:
      cursor = self.conn.cursor()
      cursor.execute(sql)
      self.conn.commit()
    except (AttributeError, MySQLdb.OperationalError):
      self.connect()
      cursor = self.conn.cursor()
      cursor.execute(sql)
      self.conn.commit()
    return cursor

  def close(self):
    if(self.cursor):
      self.cursor.close()
    #self.conn.commit()
    self.conn.close()
