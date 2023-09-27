#!/usr/bin/env python3
# location: infa01:/home/hive/bin/static_table_sync:hive
"""
解析json文件，然后生产RDMBS到HBASE的datax执行任务json文件，而后执行任务
目标是将源表数据同步到HBase中
"""
import os
import sys
import argparse
import subprocess
import json
from pyhive import presto

PRESTO_CURSOR = presto.connect(host='infa02', port=18080, username='hive').cursor()


def get_table_columns(cat, oracle_tbl):
    """
    从命令行输出解析出表字段来
    """
    PRESTO_CURSOR.execute("select * from ora_{}.{} where 1=0".format(cat, oracle_tbl))
    return [x[0].upper() for x in PRESTO_CURSOR.description]


def generate_datax_job(host, port, sid, username, password,
                       oracle_tbl, hbase_tbl, cols, is_truncate):
    #  "jdbc:oracle:thin:@10.60.1.28:1521/export"
    job = {
        "job": {
            "content": [{
                "reader": {
                    "parameter": {
                        "username": "",
                        "password": "",
                        "column": "",
                        "fetchSize": 1024,
                        "connection": []
                    },
                    "name": "oraclereader"
                },
                "writer": {
                    "parameter": {
                        "batchSize": 256,
                        "column": "",
                        "nullMode": "skip",
                        "haveKerberos": "true",
                        "kerberosPrincipal": "hive@CFZQ.COM",
                        "kerberosKeytabFilePath": "/etc/security/keytabs/hive.headless.keytab",
                        "table": "",
                        "truncate": "",
                        "hbaseConfig": {
                            "hbase.zookeeper.quorum": "dn01,dn02,dn03",
                            "zookeeper.znode.parent": "/hbase-secure"
                        }
                    },
                    "name": "hbase11xsqlwriter"
                }
                }],
            "setting": {
                "speed": {
                    "bytes": -1,
                    "channel": 1
                }
            }
        }
    }

    job['job']['content'][0]['reader']['parameter']['column'] = cols
    job['job']['content'][0]['reader']['parameter']['username'] = username
    job['job']['content'][0]['reader']['parameter']['password'] = password
    job['job']['content'][0]['writer']['parameter']['truncate'] = "true" if is_truncate else "false"
    job['job']['content'][0]['reader']['parameter']['connection'].append(
        {
            'jdbcUrl': [f"jdbc:oracle:thin:@{host}:{port}/{sid}"],
            'table': [oracle_tbl]
        })
    job['job']['content'][0]['writer']['parameter']['column'] = cols
    job['job']['content'][0]['writer']['parameter']['table'] = hbase_tbl

    return job


def process_tbl(item: dict, tbl: str, is_truncate: bool = False, source: str = "upstream"):
    """
    sync table records into hbase table

    :param dict item    information for connecting to database
    :param str tbl      table name
    :param bool is_truncate truncate table if true
    :param str source the source to sync from
    """
    print(f">>> process {tbl}")
    job_fname = "job/{}.json".format(tbl)
    oracle_tbl = tbl.upper()
    hbase_tbl = 'RT.' + oracle_tbl.replace('.', '__')
    # 生成任务
    if source == "stage":
        # 获得表字段
        cols = get_table_columns("rt", oracle_tbl)
        job = generate_datax_job("10.60.242.113", 1521, "stage", "rtreader", "rtreader20!21", oracle_tbl,
                                 hbase_tbl, cols, is_truncate)
    else:
        # 获得表字段
        cols = get_table_columns(item['class'][:2], oracle_tbl)
        job = generate_datax_job(item['host'], item['port'], item['dbname'],
                                item['user'], item['password'], oracle_tbl,
                                hbase_tbl, cols, is_truncate)
    open(job_fname, 'w').write(json.dumps(job))

    job_cmd = "/opt/infalog/datax/bin/datax.py {}".format(job_fname)
    res, output = subprocess.getstatusoutput(job_cmd)
    if res == 0:
        print(output[-380:])
        print(f"<<< finished")
        return True
    else:
        print(output)
        return False


if __name__ == '__main__':
    curdir = os.path.dirname(os.path.abspath(sys.argv[0]))
    parser = argparse.ArgumentParser(
        description='Util for syncing realtime table.')
    group = parser.add_mutually_exclusive_group(required=True)
    group.add_argument('-f', '--file', dest='filename', help='SQL from files',
                        default=curdir + '/result.json')
    group.add_argument('-t', '--table', dest='table', help='The table will be synced')


    parser.add_argument('-d', '--truncate', action='store_true', help='truncate table before write',
                        default=False)

    # 同步源，两个选择，一个是自建的实时数据库(stage)，一个是上游数据库(upstream),对应的IP地址是10.60.1.3
    parser.add_argument('-s', '--source', dest='source',
                        help="the source compared from, choose one of stage(242.113) or upstream(1.3), defaults is stage",
                        choices=["stage", "upstream"],
                        default="stage")
    args = parser.parse_args()
    if args.table:
        process_tbl({}, args.table, args.truncate, "stage")
        sys.exit(0)

    need_sync_static_table = []
    if os.path.exists(args.filename):
        try:
            need_sync_static_table = json.loads(
                open(args.filename, 'r').read())
        except Exception as e:
            print(f"failed to parse file: {args.filename}")
            print(e)
            sys.exit(1)

    for item in need_sync_static_table:
        for tbl in item['src_tables']:
            process_tbl(item, tbl, args.truncate, args.source)
