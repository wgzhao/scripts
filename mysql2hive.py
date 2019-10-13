#!/usr/bin/env python2.7
# -*- coding:utf-8 -*-
__Author__ = 'wgzhao <zhaoweiguo@haodou.com>'
'''
This script performs dumping MySQL database into Hadoop Hive environment
typical usage:
mysql2hive.py -B haodou -P 3306
'''
import os
import threading
import time
import subprocess
import logging
import argparse
import sys
import importlib
importlib.reload(sys)
sys.setdefaultencoding('utf-8')
from random import shuffle,randint
import MySQLdb
import configparser
# setup environment variable specify locale
os.environ['LANG'] = 'zh_CN.UTF-8'
os.environ['LANGUAGE '] = 'zh_CN.UTF-8'

# 默认全局参数
opts = {'host':'localhost','port':3306,'dbuser':'root','password':'password',
        'hivebasedir':'/apps/hive/warehouse'}
logpath = '/tmp/mysql2hive.log'
class MyThread(threading.Thread):
  def __init__(self,func,args,name=''):
    threading.Thread.__init__(self)
    self.name = name
    self.func = func
    self.args = args

  def getResult(self):
    return self.res

  def run(self):
    self.res = self.func(*self.args)



def execjob(db,excludetbls=None):
    '''
    import MySQL specified database to HDFS
    @param db [in] the mysql database schema
    @param port [in]  mysql connect port
    @excludetbls  list of table which exclude dumping,it is optional
    '''
    newdb = db + "_" + self.options.today
    jobname="import_%s" % db
    logging.debug("----- start job %s  ----- " % jobname)
    cmd = " sqoop import-all-tables --connect jdbc:mysql://" + self.options.host + ":" + str(self.options.port) + "/" + db + "?tinyInt1isBit=false\&autoReconnect=true"
    cmd += " --username %s --password %s  -m 3 " % \
                    (self.options.user,self.options.password)
    if excludetbls:
        cmd += " --exclude-tables '%s'" % (','.join(excludetbls))
    cmd += " --direct --hive-overwrite --hive-import --hive-database " + newdb + "  --create-hive-table "
    cmd += ' --fields-terminated-by \001 --hive-delims-replacement \002 --optionally-enclosed-by \'"\' -- --default-character-set utf8 >>%s 2>&1' % logpath
    if self.options.dryrun:
        print(cmd)
    else:
        os.system(cmd)
    logging.debug("---- job %s finished  -- " % jobname)


def create_hql(dblist,port,ctype,opts):
    '''
    create hive sql file include create or drop or both statements by ctype
    '''
    result=[]
    for db in dblist:
        newdb =  db + "_" + opts['today']
        loc = opts['hivebasedir'] + "/" + newdb
        chql = "create database %s location '%s';" % (newdb,loc)
        dhql = "drop database if exists %s cascade;" % (newdb,)
        if ctype == 'create':
            result.append(chql)
        elif ctype == 'drop':
            result.append(dhql)
        elif ctype == 'all':
            result.append(dhql)
            result.append(chql)
    return result

def import_tbls(dbname,tblnames,options):
    """
    import specify table(s) and database
    @param dbname [in] string 数据库名，这里指在MySQL里的库名
    @param tblnames [in] list 要导入的表名，这里的表名指在MysQL中的表名
    """
    hivedb =  dbname + "_" + options['today']
    loc = options['hivebasedir'] + "/" + hivedb
    cmd = "hive -S -e \"create database if not exists %s location '%s'\"" %(hivedb,loc)
    if options['dryrun']:
        print(cmd)
    else:
        subprocess.getoutput(cmd)
    #delete exists table
    cmd = 'use %s; ' % hivedb
    for tbl in tblnames:
        hivetbl = tbl.lower()
        cmd +=' drop table if exists `%s`; ' % hivetbl
    if options['dryrun']:
        print("hive -S -e '%s'" % cmd)
    else:
        subprocess.getoutput("hive -S -e '%s'" % cmd)

    for tbl in tblnames:
        #delete exists hive table
        hivetbl = tbl.lower()
        jobname = "import_%s__%s" % (dbname,hivetbl)
        cmd = " sqoop import -Dmapreduce.job.queuename=default --connect jdbc:mysql://" + options['host'] + ":" + str(options['port']) + "/" + dbname + "?tinyInt1isBit=false\&autoReconnect=true" + " --table " + tbl
        cmd += " --username %s --password %s  -m 3 --direct --warehouse-dir %s" % \
                        (options['user'],options['password'],options['hivebasedir'] + "/" + hivedb + "/")
        cmd += " --hive-overwrite --hive-import --hive-database " + hivedb + "  --create-hive-table  --hive-table " + hivetbl
        cmd += ' --fields-terminated-by \001 --hive-delims-replacement \002 --optionally-enclosed-by \'"\' -- --default-character-set utf8 --set-gtid-purged=OFF >>%s  2>&1' % logpath
        if options['dryrun']:
            print(cmd)
        else:
            logging.debug("----- start job %s  ----- " % jobname)
            subprocess.getoutput(cmd)
            logging.debug("---- job %s finished  -- " % jobname)

def main():
    threads = []
    #read tables from configfile
    #file format is like :
    #
    if self.options.configfile:
        config = configparser.ConfigParser()
        config.read(self.options.configfile)
        dbs = config.items('db')

        for item in dbs:
            db = item[0]
            tblnames = item[1].split(',')
            t = MyThread(self.import_tbls,(db,tblnames),self.import_tbls.__name__)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        return 0

    #if specify table(s)
    if self.options.tblname:
        if not self.options.dbname:
            print("specify table name(s) required specify database name")
            exit(2)
        else:
            dbs = self.options.dbname.split(',')
            if len(dbs) > 1:
                print("database requires ONLY one but get %s" % len(dbs))
                exit(3)
            else:
                self.import_tbls(self.options.dbname,self.options.tblname.split(','))
                return 0

    if self.options.dbname:
        dbs = self.options.dbname.split(',')
    else:
        #get all database except system schema
        sql = "select schema_name from information_schema.schemata where schema_name not in ('mysql','information_schema','test','performance_schema');"
        cur.execute(sql)
        dbs = [x[0] for x in self.cur.fetchall()]
    fname = 'hive_%s_hql' % self.options.port
    hql = self.create_hql(dbs,'all')
    open(fname,'w').write('\n'.join(hql))
    cmd = "hive -S -f %s" % fname
    if self.options.dryrun:
        print(cmd)
    else:
        os.system(cmd)

    for db in dbs:
        #get all subtable ,ignore theme
        if self.options.regexp:
            sql = "select table_name from information_schema.tables where table_schema = '%s' and table_name not REGEXP '%s';" % (db,self.options.regexp)
            try:
                self.cur.execute(sql)
                tblnames = [x[0] for x in self.cur.fetchall()]
            except Exception as err:
                print("failed to execute sql %s : %s " % (sql,err))
                exit(1)

        t = MyThread(self.import_tbls,(db,tblnames),self.import_tbls.__name__)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()


if __name__ == '__main__':

    parse = argparse.ArgumentParser(description='Dump MySQL database(s) into Hive database',
            prog='PROG',
            formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    parse.add_argument('-b','--basedir',dest='hivebasedir',help='the hive base directory',default='/apps/hive/warehouse/')
    parse.add_argument('-u','--user',dest='user',help='User for login to MySQL Server,default is root',default='root')
    parse.add_argument('-H','--host',dest='host',help='Connect to host,default is 10.0.10.85',default='localhost')
    parse.add_argument('-p','--password',dest='password',help='Password to use when connect to MySQL Server,the default is password',default='password')
    parse.add_argument('-P','--port',dest='port',help='Port number to use for connection ,the default is 3306 ',default=3306,type=int)
    parse.add_argument('-B','--db',nargs='+',dest='dbname',help='databases name to import,if not given,will import all normal databases',default=None)
    #parse.add_argument('-t','--day',dest='today',help='date,the format is %%Y%%m%%d,take it as the suffix of hive database name ,current date is default if not given',default=curdate)
    parse.add_argument('-T','--table',nargs='+',dest='tblname',help='tables whose will dump,it required specify -B option')
    parse.add_argument('-x','--regexp',dest='regexp',help='Regular expression for exclude tables, the defualt will excludes all table names end with _[0-9a-f][0-9a-f]',default='_[0-9a-f]{2}$')
    parse.add_argument('-n','--dry-run',dest='dryrun',action='store_true',help='Just only print execute instructon instead of execute it')
    parse.add_argument('-f','--file',nargs=1,dest='configfile',help='read db(s) and table(s) from specified file and dumps into hive')

    args = parse.parse_args()

    print("view %s for details" % logpath)

    logging.basicConfig(filename = logpath,format='%(levelname)s:%(asctime)s %(msg)s',datefmt = '%Y-%m-%d %H:%M:%S',level=logging.DEBUG)

    #merge default options and command-argument options
    opts.update(args.__dict__)

    threads = []

    #read tables from configfile
    #file format is like :
    #
    if opts['configfile']:
        config = configparser.ConfigParser()
        config.read(opts['configfile'])
        dbs = config.items('db')
        options = opts.copy()
        for item in dbs:
            db = item[0]
            tblnames = item[1].split(',')
            t = MyThread(import_tbls,(db,tblnames,options),import_tbls.__name__)
            threads.append(t)
            t.start()

        for t in threads:
            t.join()
        exit(0)

    #if specify table(s)
    if opts['tblname']:
        if not opts['dbname'] or len(opts['dbname']) >1 :
            print("specify table name(s) MUST required specify ONLY one database name")
            exit(2)
        else:
            import_tbls(opts['dbname'][0],opts['tblname'],opts)
            exit(0)


    conn =  MySQLdb.connect(host=opts['host'],user=opts['user'],passwd=opts['password'],port=opts['port'])
    cur = conn.cursor()
    cur.execute("set names 'utf8'")

    if opts['dbname']:
        dbs = opts['dbname']
    else:
        #get all database except system schema
        sql = "select schema_name from information_schema.schemata where schema_name not in ('mysql','information_schema','test','performance_schema');"
        cur.execute(sql)
        dbs = [x[0] for x in cur.fetchall()]

    fname = 'hive_%s.hql' % opts['port']
    hql = create_hql(dbs,opts['port'],'all',opts)
    open(fname,'w').write('\n'.join(hql))
    cmd = "hive -S -f %s" % fname
    #os.system(cmd)

    for db in dbs:
        #get all subtable ,ignore theme
        if opts['regexp']:
            sql = "select table_name from information_schema.tables where table_schema = '%s' and table_name not REGEXP '%s';" % (db,opts['regexp'])
            try:
                cur.execute(sql)
                tblnames = [x[0] for x in cur.fetchall()]
            except Exception as err:
                print("failed to execute sql %s : %s " % (sql,err))
                exit(1)

        t = MyThread(import_tbls,(db,tblnames,opts),import_tbls.__name__)
        threads.append(t)

    for t in threads:
        t.start()

    for t in threads:
        t.join()
