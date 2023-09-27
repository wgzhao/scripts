#!/usr/bin/env python3
# location: infa01:/home/hive/bin/static_table_sync:hive
"""
比较实时表的表字段差异，用于当源表结构有变动时，可以快速获得修改表结构的DDL语句
而不用手工去一个一个对比
从指定的json文件读取源库的连接方式以及对应的表
然后和特定的HBase中的表进行结构对比
比较策略：
1. HBASE表的字段可以比源表字段多
2. HBASE表字段的顺序可以和源表不一致（源表可能存在中间加字段的情况，而HBASE表不允许中间加字段）
3. 源表字段不在HBASE表中的，增加该字段
4. 源表字段类型有变更的，按照变动规则进行修改
5. 源表中字段有删除的，HBASE表删除对应字段
"""
import json
import sys
from typing import List, Dict, Optional, Tuple, Any

from cxzq_utils.msgsender import MsgSender
from pyhive import presto
from pyhive.exc import DatabaseError

# connect presto
dest_cursor = presto.connect(host='nn01', port=59999, username='hive', catalog='hbase', schema='rt').cursor()
mobiles = "15974103570"
sender = MsgSender()


def gen_create_table(metadata: list, table: str) -> str:
    """generate create table sql

    :param list_or_tuple metadata: the list of table's columns
    :param str table: table name
    :returns: the sql of creating table
    """
    sql = "create table rt.{} (".format(table)
    cols = []
    for x in metadata:
        if x[1].startswith('datetime') or x[1].startswith('varchar') or x[1].startswith('char'):
            cols.append(x[0] + " varchar")
        else:
            cols.append(x[0] + " {}".format(x[1]))
    sql += ','.join(cols)
    sql += ')COLUMN_ENCODED_BYTES=0, DISABLE_WAL=true;'
    return sql


def drop_table_column(table: str = None, column: str = None) -> str:
    """
    Generate DROP COLUMN DDL via specified table and column
    """
    sql = "ALTER TABLE {} DROP COLUMN {};".format(table, column)
    sender.send_kk(sql, "table {} drop column {}".format(table, column), mobiles)
    return sql


def drop_and_add_column(table: str = None, column: str = None, col_type: str = None) -> str:
    """
    modify column type
    """
    sql = "ALTER TABLE {table} DROP COLUMN {col};\n\t\tALTER TABLE {table} ADD {col} {col_type};".format(
        table=table, col=column, col_type=col_type)
    sender.send_kk(sql, "table {} alter column {}".format(table, column), mobiles)
    return sql


def add_column(table: str = None, column: Dict = None) -> str:
    if column["type"] in ["char", "varchar", "date", "timestamp"]:
        sql = "ALTER TABLE {} ADD {} VARCHAR;".format(table, column["name"])
    else:
        sql = "ALTER TABLE {} ADD {} {};".format(table, column["name"], column["stype"])
    sender.send_kk(sql, "table {} add column {}".format(table, column["name"]), mobiles)
    return sql


def get_meta(desc: Optional[List[Tuple[Any, Any, None, None, None, None, bool]]]) -> dict:
    table_meta = {}
    for item in desc:
        table_meta[item[0]] = {"name": item[0], "type": None, "precision": 0, "scale": 0, "stype": item[1]}
        stype = item[1]
        if '(' in stype:
            col_type, length = stype.split('(')
            table_meta[item[0]]["type"] = col_type
            if ',' in length:
                precision, scale = length[:-1].strip().split(",")
                table_meta[item[0]]["precision"] = int(precision)
                table_meta[item[0]]["scale"] = int(scale)
            else:
                table_meta[item[0]]["precision"] = int(length[:-1].strip())
        else:
            table_meta[item[0]]["type"] = stype
    return table_meta


def check_tbl(ora_cur: presto.Cursor, table: str):
    """
    check the table schema's difference between oracle and hbase

    :param object ora_cur: pyhive connect object
    :param str table: table name which be checked
    :returns: none
    """
    # get table's columns
    hbase_tbl = table.replace('.', '__')
    print("\tprocess table rt.{}".format(hbase_tbl))
    try:
        ora_cur.execute("select * from {} where 1=0".format(table))
        ora_meta = get_meta(ora_cur.description)
    except DatabaseError as e:
        print("failed query origin table {}: {}".format(hbase_tbl, e))
        return
    dest_cursor.execute("select * from {} where 1=0".format(hbase_tbl))

    hbase_meta = get_meta(dest_cursor.description)

    for col in ora_meta.keys():
        ora_column = ora_meta[col]
        if col not in hbase_meta:
            # add column
            # print("\t\tcolumn[{}({})] does not exists , add it".format(col, ora_column["stype"]))
            print("\t\t{}".format(add_column(hbase_table, ora_column)))
            continue

        hbase_column = hbase_meta[col]
        ora_type = ora_column["type"]
        hbase_type = hbase_column["type"]

        # timestamp 一般转成了varchar，如果类型是varchar不带长度，那就不再需要比较
        if hbase_type == "varchar":
            if hbase_column["precision"] == 0:
                continue
            elif ora_type == "varchar" and ora_column["precision"] > hbase_column["precision"]:
                # print("\t\tcolumn[{}] has changed from {}({}) to {}({})".format(col, hbase_type, hbase_column["stype"], ora_type, ora_column["stype"]))
                print("\t\t{}".format(drop_and_add_column(hbase_tbl, col, "varchar")))
                continue
            continue

        if ora_type == hbase_type and (
                hbase_column["precision"] < ora_column["precision"] or hbase_column["scale"] < ora_column["scale"]):
            # print("\t\tcolumn[{}} has changed from {} to {}".format(col, ora_column["stype"], hbase_column["stype"]))
            print("\t\t{}".format(drop_and_add_column(hbase_tbl, col, ora_type)))
            continue

        if ora_type != hbase_type:
            print("\t\tcolumn[{}] type has changed from {} to {}".format(col, hbase_type, ora_type))
            if ora_column["type"] in ["char", "varchar", "date", "timestamp"]:
                print("\t\t{}".format(drop_and_add_column(hbase_tbl, col, "varchar")))
            else:
                print("\t\t{}".format(drop_and_add_column(hbase_tbl, col, ora_column["stype"])))
            continue


if __name__ == '__main__':

    if len(sys.argv) > 1:
        fpath = sys.argv[1]
    else:
        fpath = './all_tbls.json'

    content = json.loads(open(fpath, 'r').read())
    for item in content:
        print("process database {}".format(item['class']))
        src_cur = presto.connect(host='infa02', port=18080, username='hive',
                                 catalog='ora_{}'.format(item['class'][:2])).cursor()
        for tbl in item['src_tables']:
            check_tbl(src_cur, tbl)
