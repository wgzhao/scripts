#!/usr/bin/env python
# encoding=utf8
import sys
import importlib
importlib.reload(sys)

sys.setdefaultencoding('utf-8')

import MySQLdb 
import pyhs2
import argparse
import sys
import csv
from os import remove
#tbl_search_re='rpt_*|ods_qnc_citysite|ods_qnc_page_code_ds'
columns_map = {'STRING_TYPE':'varchar(512)','DOUBLE_TYPE':'float','INT_TYPE':'int','TIMESTAMP_TYPE':'timestamp'}
DEBUG=False
def list_tables(hivecurl):
    
    """list all tables will dumping"""
    hql = "show tables in bing like '%s'" % tbl_search_re
    hivecur.execute(hql)
    for tbl in hivecur.fetch():
        print(tbl[0], end=' ')
    
if __name__ == '__main__':
   
    parser = argparse.ArgumentParser(description='dump  specified table(s)  from hive database into mysql database.')
    parser.add_argument('--tables',dest='tbls',help="the lists of table will dump,separator by comma ,e.g 'spt_app_index,spt_rpt_dm'")
    parser.add_argument('--regex',dest='tbl_search_re',help='tables using to dump ,using regular expression. It will INGORE when specify --tables option',default='rpt_*|ods_qnc_citysite|ods_qnc_page_code_ds')
    parser.add_argument('--list',dest='listbls',action='store_true',help='list all tables will dumping then exit')
    parser.add_argument('--host',dest='myhost',help='mysql host to connect',default='10.1.1.50')
    parser.add_argument('--port',dest='myport',help='Port number to use for connect to server',default=3306)
    parser.add_argument('--user',dest='myuser',help='user to connect mysql',default='imp')
    parser.add_argument('--password',dest='mypasswd',help='Password to use when connect to server.',default='imp123')
    parser.add_argument('--database',dest='mydbname',help='database to use',default='bing')
    parser.add_argument('--verbose',dest='verb',action='store_true',help='print more detail')
  
    args = parser.parse_args()
    if args.tbls and args.tbl_search_re:
        print("ONLY specify one between --tables and --regex")
        exit(1)
        
    if args.verb:
        DEBUG=True
        
    hiveconn = pyhs2.connect(host='10.1.1.12',
                       port=10000,
                       authMechanism="NOSASL",
                       database='bing')
    hivecur = hiveconn.cursor()

    myconn = MySQLdb.connect(host=args.myhost,port=args.myport,user=args.myuser,passwd=args.mypasswd,db=args.mydbname)
    #test env
    #myconn = MySQLdb.connect(host='127.0.0.1',port=3306,user='root',passwd='',db='test')

    mycur = myconn.cursor()
    
    if args.listbls:
        list_tables(hivecur)
        exit(0)

        
    if not args.tbls:
        #use default table
        if DEBUG:
            print("get all tables in database bing") 
        hql = "show tables in bing like '%s'" % args.tbl_search_re
        hivecur.execute(hql)
        dump_tables = [x[0] for x in hivecur.fetch() ]
    else:
        dump_tables = args.tbls.split(',')
        
    for tbl in dump_tables:
        if DEBUG:
            print("%s begin handling table %s %s" % (8*'*',tbl,8*'*'))
            print("Step 1. get all data and table schema")
        hql = "select * from %s" % tbl
        hivecur.execute(hql)
        

        # get all records and write to csv-format file
        records = [x for x in  hivecur.fetch() ]
        filepath='/tmp/%s.dat' % tbl
        f = open(filepath,'w')
        writer = csv.writer(f)
        writer.writerows(records)
        f.close()
        
        if DEBUG:
            print("Step 2. assembly SQL statement")
            
        #get table schema,assembly create statement according to hive schema
        tbl_schema = hivecur.getSchema()
        create_table_sql = 'create table %s (' % tbl
        for schema in tbl_schema:
            #colname form of <table>.<column> ,strip table name
            colname = schema['columnName'].split('.')[-1] 
            create_table_sql += "%s %s comment '%s'," % (colname,columns_map[schema['type']],schema['comment'])
            
        create_table_sql =create_table_sql[:-1] +  ") ENGINE=InnoDB DEFAULT CHARSET=utf8;"
        
        if DEBUG:
            print("create statement: %s " % create_table_sql)
            #print "load data statement: %s " % insert_sql
        
        if DEBUG:
            print("Step 3. load data into %s.%s" % (args.mydbname,tbl))
        mycur.execute('drop table if exists %s' % tbl)
        mycur.execute(create_table_sql)
        #mycur.executemany(insert_sql,records)
        mycur.execute("load data local infile '/tmp/%s.dat' into table %s.%s fields terminated by ','" % (tbl,args.mydbname,tbl))
        remove(filepath)
        if DEBUG:
            print("table %s finished" % tbl)
        
    myconn.commit()
        
        
