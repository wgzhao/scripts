#!/usr/bin/env python
# -*- encoding:utf-8 -*-
import sys
import MySQLdb
import time
import smtplib
from email import Encoders
from email.mime.base import MIMEBase
from email.mime.multipart import MIMEMultipart
from email.mime.text import MIMEText
from email.header import Header
from jinja2 import Template
from subprocess import getoutput
import sqlite3

class DBValidate():
    
    """docstring for DBValidate"""
    
    def __init__(self,host='127.0.0.1',port=25):
        
        #super(DBValidate, self).__init__()
        #self.arg = arg

        self.emailHost = host
        self.emailPort = port

        self.template = Template('''
          <!DOCTYPE HTML PUBLIC "-//W3C//DTD HTML 4.01 Transitional//EN"
              "http://www.w3.org/TR/html4/loose.dtd">
            <head>
                <meta http-equiv="Content-type" content="text/html; charset=utf-8">
                <title>database validation report</title>
                <body id="db_validate" onload="">
                    {% for key,val in sc.iteritems() %}
                    <div id="{{ key |e  }}">
                        <table border="1" cellspacing="1" cellpadding="1">
                            <caption>database instance {{ key |e }}</caption>
                            <tr><th>Database Name</th><th>table numbers(MySQL)</th><th>table numbers(Hive)</th></tr>
                            {% for db in val %}
                            <tr {% if db[1] != db[2] %} style="background-color:red" {% endif %} ><td>{{db[0]}}</td><td>{{db[1]}}</td><td>{{db[2]}}</td></tr>
                            {% endfor %}
                        </table>
                    </div>
                    <p></p>
                    {% endfor %}
                </body>
            </head>
            ''')
    
        # dbsResult is nest dictionary
        '''
        {3306:[['haodou_center',1,1],[haodou_admin',3,3]],
         3307:[['recipe',24,23],['haodou_mobile',30,30]],
         3308:[['qunachi_user',3,3]]
        }
        '''
        self.dbsResult={}
        self.dbInsts = {3306:'hd_',3307:'rcp_'}
        for port in list(self.dbInsts.keys()):
            self.dbsResult[port] = []
       
            
        # connect hive metastore 
        self.hiveconn = MySQLdb.connect(host='10.1.1.12',user='ro',passwd='ro123')
        self.hivecur = self.hiveconn.cursor()
        self.ymd = time.strftime('%Y%m%d')
        
    def get_db_tbl_count(self,port=3306):
        
        conn = MySQLdb.connect(host='10.1.1.70',user='hd_backup',passwd='yf1013',port=port)
        cur = conn.cursor()
        cur.execute('''select table_schema,count(table_name) as tblcnt  
                    from information_schema.tables 
                    where table_name not REGEXP '_[0-9a-f][0-9a-f]$' 
                    and  table_type = 'BASE TABLE' 
                    and table_schema not in ('mysql','test','performance_schema','information_schema') 
                    group by table_schema order by table_schema;''')
        templist=[]
        for row in cur.fetchall():

            # get the number of hive corresponding

            hivedb = self.dbInsts[port] + row[0] + "_" + self.ymd
            self.hivecur.execute( '''
                        select count(tbl_name) as tblcnt from hive.TBLS where DB_ID  in (select DB_ID from hive.DBS where name = '%s');
                        ''' % hivedb)

            templist.append([row[0],row[1],self.hivecur.fetchone()[0]])
            
        self.dbsResult[port] = templist

          
    
    def validate(self):
        '''
        validate hive database and mysql database
        pick up random 10 tables and validate theme
        '''
        #pick up sample tables 
        sql = "select table_schema,table_name from information_schema.tables where table_schema not in ('mysql','test','information_schema','performance_schema') "
        if self.options.regexp:
            sql += " and table_name not  REGEXP '%s'" % self.options.regexp
        sql += "  order by rand() limit 20;"
        logging.debug("\t\t\t\t\t-------------- validate -------------")
        logging.debug("\t\t\t \t\t MySQL \t\t Hive")
    
        self.cur.execute(sql)
    
        for rs in self.cur.fetchall():
            schema = rs[0]
            tbl = rs[1]
            sql = "select count(1) from %s.%s;" % (schema,tbl)
            self.cur.execute(sql)
            mysql_cnt = self.cur.fetchone()[0]
            hql = "select count(1) from %s.%s;" % (self.options.dbprefix + schema + "_" + self.options.today, tbl.lower())
            cmd = "hive -S -e '%s' 2>>/tmp/query_hive.log" % hql 
            hive_cnt = commands.getoutput(cmd)
            logging.debug("%s.%s \t\t %s \t\t %s" % (schema,tbl,mysql_cnt,hive_cnt))
            
    def svalidate(self):
        '''
        数据校验，初步设想是，针对数据库的每个表，随机抽取若干记录，然后取每个记录的随机字段，用该字段的值作为查询条件，
        分别在MySQL和Hive中查询，看获得的记录数是否一致，如果一致，则可以认为该表的数据是一致的。
        '''
        sql = "select table_schema,table_name from information_schema.tables where table_schema not in ('mysql','test','information_schema','performance_schema') "
        if self.options.regexp:
            sql += " and table_name not  REGEXP '%s'" % self.options.regexp
        # TODO: 去掉一些老旧的，无需校验的表
    
        self.cur.execute(sql)
        tbls=self.cur.fetchall()
        logging.debug("\t--------------content validation -------------")
    
        for rs in tbls:
            schema = rs[0]
            tbl = rs[1]
            #logging.debug("validate table: %s" % tbl)
            #获得表字段名称，去掉text，datetime等类型
            sql="select ordinal_position,column_name from information_schema.columns where table_name = '%s' and table_schema = '%s' \
                    and data_type not in ('datetime','tinyint','text','enum') order by ordinal_position ;" % (tbl,schema)
            self.cur.execute(sql)
            cols = dict()   #保存当前表的字段顺序以及字段名
            for row in self.cur.fetchall():
                cols[row[0] -1 ] = row[1] #row[0] is column order ,start from 1 and row[1] is column name
            col_len = len(cols)
            col_keys = list(cols.keys())    
            #获得该表的数据记录数
            sql = "select %s from %s.%s order by rand() limit 20" % (','.join(list(cols.values())),schema,tbl)
            self.cur.execute(sql)
            record_rows = self.cur.fetchall()
            validate_pass = True
            for row in record_rows:
                #从记录中随机取一个字段的值
                try:
                    shuffle(col_keys)
                    rand_col_pos = col_keys[0]
                    rand_col_name = cols[rand_col_pos]
                    rand_col_value = row[rand_col_pos]
                except Exception as err:
                    print("key error on table %s.%s : %s " % (schema,tbl,err))
                    print(col_keys)
                    print("rand_col_pos is %d" % rand_col_pos)
                    continue
                
                if rand_col_value == '' or (isinstance(rand_col_value,str) and len(rand_col_value) > 100):   #maybe it is serial-data,skip it
                    continue
                #logging.debug("valid column:%s" % rand_col_name)
                sql="select count(1) from %s.%s where %s = '%s'" % (schema,tbl,rand_col_name,rand_col_value)
                #logging.debug("mysql sql: %s" % sql)
                self.cur.execute(sql)
                my_cnt = self.cur.fetchone()[0]
                hive_db = self.options.dbprefix + schema + "_" + self.options.today
                hql = "select count(1) from %s.%s where %s = '%s'" % (hive_db,tbl,rand_col_name,rand_col_value)
                #logging.debug("hive hql: %s" % hql)
                cmd = "shark -skipRddReload -S -e \"%s\" 2>>/tmp/query_shark.log |tail -n1" % hql 
                #cmd = "hive -S -e '%s' 2>>/tmp/query_hive.log " % hql 
                ret,hive_cnt = commands.getstatusoutput(cmd)
                if int(ret) > 0:
                    logging.debug('failed to exec hql,retcode:%s' % ret)
                elif my_cnt != hive_cnt:
                    logging.debug("valid failure: schema:%s table:%s column:%s value:%s" %(schema,tbl,rand_col_name,rand_col_value))
                    logging.debug("hql: %s" % hql)
                    logging.debug("mysql cnt: %d  \t hive cnt: %s" % (my_cnt,hive_cnt))
                    validate_pass = False
                    break
            if validate_pass == True:
                logging.debug("table %s.%s validate pass" % (schema,tbl))
            else:
                logging.debug("table %s.%s validate pass" % (schema,tbl))
        logging.debug("\t--------------end  validation -------------")
    
    def render_report(self):
         return  self.template.render(sc = self.dbsResult)
    
    def send_mail(self,title,content,toEmail):

        msg = MIMEMultipart('alternative')
        msg['Subject'] = title
        msg['Content-Type'] = "text/html; charset=utf-8"
        msg['From'] = 'noreply@notice1.haodou.com'
        #msg['To'] = toEmail
        msg['To'] = ', '.join(toEmail) #';'.join(toEmail)
        part = MIMEText(content, _subtype='html',_charset='utf-8')
        msg.attach(part)
        s = smtplib.SMTP(self.emailHost,self.emailPort)
        #s.login(mail_username, mail_password)
        #print msg.as_string()
        s.sendmail('noreply@notice1.haodou.com', toEmail,msg.as_string())
        s.quit()
        
def getHiveTableCnt():
    '''
    get the number of records of each table dumping at current.
    '''    
    ymd = time.strftime('%Y%m%d')
    
    res = []
    
    #step 1. get all database belongs to current date
    dbs = getoutput("hive -S -e \"show databases like '*_%s'\" 2>/dev/null" % ymd).split('\n')

    for db in dbs:
        # step 2. get all tables belongs to specified table
        tbls = getoutput("hive -S -e 'show tables in %s' 2>/dev/null" % db).split('\n')
        for tbl in tbls:
            #step 3. get the number of records in the table 
            cnt = getoutput("hive -S -e 'select count(*) from %s.%s' 2>/dev/null" % (db,tbl))
            #step 4. insert into res list using tuple style
            res.append("('%s','%s','%s','%s')" % (db.replace('_'+ymd,''),tbl,ymd,cnt))
    
    #print result for save 
    print(res)
    #step 5. dump result into sqlite 
    conn = sqlite3.connect('/home/weiguo/hivetbl.db')
    c = conn.cursor()
    for t in res:
        c.execute('insert into tbl_statis values(?,?,?,?)',t)
    c.commit()
    c.close()    
     
    
def main():
    toEmail = ['zhaoweiguo@haodou.com','dc@haodou.com']
    dv = DBValidate()
    
    for port in list(dv.dbInsts.keys()):
        dv.get_db_tbl_count(port)

    html = dv.render_report()
    dv.send_mail("Database validate report",html,toEmail)
    
if __name__ == '__main__':
    
    main()
    #getHiveTableCnt()
    