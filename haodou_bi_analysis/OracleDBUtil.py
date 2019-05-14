import cx_Oracle

class OracleDB:
  conn = None
  cursor = None
  def connect(self):
    #self.conn = MySQLdb.connect (host='192.168.1.118', user='bi',passwd='bi_haodou',db='haodou_server',charset="utf8")
  	self.conn = cx_Oracle.connect("dw/dw@192.168.1.32:1521/dw")


  def execute(self, sql):
    try:
      cursor = self.conn.cursor()
      cursor.execute(sql)
    except Exception:
      self.connect()
      cursor = self.conn.cursor()
      cursor.execute(sql)
    return cursor

  def close(self):
    if(self.cursor):
      self.cursor.close()
    self.conn.commit()
    self.conn.close()
