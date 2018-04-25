#!/usr/bin/env python2.7
# encoding=utf8
import sys
reload(sys)

sys.setdefaultencoding('utf-8')

import MySQLdb
import pyhs2
import argparse
import sys
import csv
import pandas as pd
from os import remove
from sqlalchemy import *
from Queue import Queue
from threading import Thread
import commands
num_worker_threads = 4

tbl_search_re='rpt_app_*|rpt_grp_*|rpt_haodou_*|rpt_hoto_*|rpt_rcp_*|dw_app_function'
columns_map = {'STRING_TYPE':'varchar(512)','DOUBLE_TYPE':'float','INT_TYPE':'int','TIMESTAMP_TYPE':'timestamp'}
VERBOSE=False
global hivecur
global sqlengine,metahive
def list_tables():

    """list all tables will dumping"""
    hql = "show tables in bing like '%s'" % tbl_search_re
    hivecur.execute(hql)
    for tbl in hivecur.fetch():
        print tbl[0],

def imp_tbl(q):
    while not q.empty():
        tbl = q.get()
        if VERBOSE:
            print "%s begin handling table %s %s" % (8*'*',tbl,8*'*')
            print "Step 1. get table schema"

        #从hive元数据中（这里是MySQL）获得表的字段信息，并进行替换和拼接
    	create_table_sql="create table bing.%s (" % tbl
        cols1 = metahive.execute("select concat(c.column_name,' ',(case when c.type_name = 'string' then 'varchar(512)' else c.type_name end)) as col from DBS d join TBLS t   on d.db_id=t.db_id join SDS s   on t.sd_id=s.sd_id join COLUMNS_V2 c on s.cd_id=c.cd_id   where d.name='bing' and t.tbl_name = '%s' order by c.integer_idx asc;" % (tbl,)).fetchall()


    	#除了正规字段外，因为不想在MySQL中创建分区表，因此把Hive中分区字段也纳入到表字段中
        cols2 = metahive.execute("select concat(p.pkey_name,' ',(case when p.pkey_type = 'string' then 'varchar(512)' else p.pkey_type end)) as col from DBS d join TBLS t   on d.db_id=t.db_id join SDS s   on t.sd_id=s.sd_id join PARTITION_KEYS p on p.tbl_id = t.tbl_id  where d.name='bing' and t.tbl_name = '%s' order by p.integer_idx asc;" % (tbl,)).fetchall()

        col = cols1 + cols2
        for c in col:
            create_table_sql += c[0] + ','
    	#拼接建表语句组后一部分，注意要删除多余的,
    	create_table_sql= create_table_sql[:-1] +") ENGINE=InnoDB DEFAULT CHARSET=utf8;"

        if VERBOSE:
            print "create statement: %s " % create_table_sql
            #print "load data statement: %s " % insert_sql

        if VERBOSE:
            print "Step 2. get all table data"
        #hql = "select * from %s" % tbl
        #hivecur.execute(hql)
        filepath='/tmp/%s.dat' % tbl
        cmd = "hive -S -e 'select * from bing.%s' 2>/tmp/hive_export_%s.log  |grep -v '^WARN:' >%s" % (tbl,tbl,filepath)
        commands.getoutput(cmd)
        # get all records and write to csv-format file
        #records = [x for x in  hivecur.fetch() ]

        #pd.DataFrame([ x for x in  hivecur.fetch() ]).to_csv(filepath, header=False, index=False, sep=',')
        #f = open(filepath,'w')
        #writer = csv.writer(f)
        #writer.writerows(records)
        #f.close()

        if VERBOSE:
            print "Step 3. load data into mysql server"
        # mycur.execute('drop table if exists %s' % tbl)
        # mycur.execute(create_table_sql)
        sqlengine.execute('drop table if exists %s' % tbl)
        sqlengine.execute(create_table_sql)
        #mycur.executemany(insert_sql,records)
        #mycur.executemany(insert_sql,records)
        #sqlengine.execute("load data local infile '/tmp/%s.dat' into table %s fields terminated by '\t'" % (tbl,tbl))
        cmd = "mysqlimport -uimp -h10.0.11.50 -pimp123 --fields-terminated-by='\t' --delete --local bing %s " % filepath
        commands.getoutput(cmd)
        if VERBOSE:
            print "table %s finished" % tbl
        remove(filepath)
    # myconn.commit()

def export_mysql(args):

    if args.regtable:
        hql = "show tables in bing like '%s'" % args.regtable
        hivecur.execute(hql)
        dump_tables = [x[0] for x in hivecur.fetch() ]

    elif not args.tbls:
        #use default table
        if VERBOSE:
            print "get all tables in database bing"
        hql = "show tables in bing like '%s'" % tbl_search_re
        hivecur.execute(hql)
        dump_tables = [x[0] for x in hivecur.fetch() ]
    else:
        dump_tables = args.tbls

    q = Queue()
    for tbl in dump_tables:
        q.put(tbl)
    threads = []
    for i in range(num_worker_threads):
        t = Thread(target=imp_tbl,args=(q,))
        t.daemon = True
        threads.append(t)

    for t in threads:
        t.start()
    for t in threads:
        t.join()


    print "All DONE"


if __name__ == '__main__':

    parser = argparse.ArgumentParser(description='dump  specified table(s)  from hive database into mysql database.')
    parser.add_argument('-t','--tables',nargs='*',dest='tbls',help="the lists of table will dump")
    parser.add_argument('-x','--reg_table',dest='regtable',help="the lists of table will dump, regex ,e.g 'spt_app_*'")
    parser.add_argument('-l','--list',dest='listbls',action='store_true',help='list all tables will dumping then exit')
    parser.add_argument('-H','--threads',dest='th',type=int,help='the number of threads,default is 4')
    parser.add_argument('-v','--verbose',dest='verb',action='store_true',help='print more detail')

    args = parser.parse_args()

    if args.verb:
        VERBOSE=True
    if args.th:
        num_worker_threads = args.th
    hiveconn = pyhs2.connect(host='10.0.11.12',
                       port=10000,
                       authMechanism="NOSASL",
                       database='bing')
    hivecur = hiveconn.cursor()
    # myconn = MySQLdb.connect(host='10.0.11.50',port=3306,user='imp',passwd='imp123',db='bing')
    #test env
    #myconn = MySQLdb.connect(host='127.0.0.1',port=3306,user='root',passwd='',db='test')

    # mycur = myconn.cursor()
    sqlengine = create_engine('mysql://imp:imp123@10.0.11.50:3306/bing?charset=utf8', pool_recycle=30)
    metahive = create_engine('mysql://ro:ro123@10.0.11.12:3306/hive?charset=utf8', pool_recycle = 5)
    if args.listbls:
        list_tables()
        exit(0)

    export_mysql(args)
