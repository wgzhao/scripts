#!/usr/bin/env python3
"""
创建基于 DolphinScheulder 的工作流，该工作流主要是完成数据库到 HDFS 的数据同步工作。
其过程是：
1. 根据指定的数据库连接信息，获取数据库中所有的表名以及字段定义
2. 生成对应的 Hive 表定义以及增加分区语句
3. 生成 Addax 的配置文件
4. 创建 DolphinScheulder 的工作流，工作流包含自动建表，增加分区，数据同步等任务
"""
import sys
import requests
import json
from sqlalchemy import create_engine, inspect, MetaData, Table, Column
from sqlalchemy.sql.ddl import DDL
from sqlalchemy.types import Integer, String, BigInteger, Float, Numeric,Date, DateTime
from typing import List
import sqlalchemy
if sys.version_info < (3, 2):
  from optparse import OptionParser as ArgumentParser
else:
  from argparse import ArgumentParser


base_url = "http://etl02:12345/dolphinscheduler"

# token 可以从 DS 后台管理中的安全中心--令牌管理中创建
headers = {
    "Content-Type": "application/json",
    "token": "1681489a77ef9e7c08077f6782e2f630"
}

class DB2Hive:
  """
  """
  # addax mysql2hdfs template
  hadoopConfig = {
            "dfs.nameservices": "yytz",
            "dfs.ha.namenodes.yytz": "nn1,nn2",
            "dfs.namenode.rpc-address.yytz.nn1": "nn01.bigdata.gp51.com:8020",
            "dfs.namenode.rpc-address.yytz.nn2": "nn02.bigdata.gp51.com:8020",
            "dfs.client.failover.proxy.provider.yytz": "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider"
        }
  addax_mysql2hdfs_template = {
      "job": {
        "setting": {
          "speed": {
            "byte": -1,
            "channel": 1
          }
        },
        "content":
          {
            "reader": {
              "name": "mysqlreader",
              "parameter": {
                "username": "",
                "password": "",
                "column": [
                  "*"
                ],
                "connection": [
                  {
                    "table": [],
                    "jdbcUrl": []
                  }
                ],
                "where": ""
              }
            },
            "writer": {
              "name": "hdfswriter",
              "parameter": {
                "defaultFS": "hdfs://yytz",
                "fileType": "orc",
                "fileName": "addax",
                "path": "/ods/odszk/checkinout/logdate=${system.biz.curdate}",
                "column": [],
                "hadoopConfig": hadoopConfig,
                "writeMode": "overwrite",
                "fieldDelimiter": "\u0001",
                "compress": "lz4"
              }
            }
          }
      }
      }

  def __init__(self, project_id, host, port, user, password, db, hive_db):
    self.db = db
    self.project_id = project_id
    self.ds_url = base_url + '/projects/{}'.format(project_id)
    self.hive_db = hive_db
    headers["Content-Type"] = "application/x-www-form-urlencoded"

    if password is None or password == "":
      jdbcUrl = "jdbc:mysql://{}@{}:{}/{}".format(user, host, port, db)
      db_url = "mysql+mysqlconnector://{}@{}:{}/{}".format(user, host, port, db)
    else:
      jdbcUrl = "jdbc:mysql://{}@{}:{}/{}".format(user, host, port, db)
      db_url = "mysql+mysqlconnector://{}:{}@{}:{}/{}".format(user, password, host, port, db)
      self.addax_mysql2hdfs_template['job']['content']['reader']['parameter']['password'] = password

    self.addax_mysql2hdfs_template['job']['content']['reader']['parameter']['connection'][0]['jdbcUrl'] = [jdbcUrl]
    self.addax_mysql2hdfs_template['job']['content']['reader']['parameter']['username'] = user

    self.engine = create_engine(db_url)

  def get_all_tables(self) -> List[str]:
    inspector = inspect(self.engine)
    return inspector.get_table_names()

  def generate_hive_table_ddl(self, table_name: str) -> str:
    self.addax_mysql2hdfs_template['job']['content']['reader']['parameter']['connection'][0]['table'] = [table_name]
    # append table to hdfs path
    self.addax_mysql2hdfs_template['job']['content']['writer']['parameter']['path'] = "/ods/" + self.hive_db + "/" +  table_name + "/logdate=${system.biz.td}"
    table: Table = Table(table_name, MetaData(), autoload_with=self.engine)
    hive_sql: list[str] = []
    hive_sql.append("create database if not exists {0} location '/ods/{0}'".format(self.hive_db))
    ddl = "CREATE EXTERNAL TABLE IF NOT EXISTS {}.{} (".format(self.hive_db, table_name)
    hive_column = []
    for column in table.columns:
      hive_type = self.convert_mysql_to_hive_type(column)
      ddl += "{} {},".format(column.name, hive_type)
      hive_column.append({"name": column.name, "type": hive_type})
    self.addax_mysql2hdfs_template['job']['content']['writer']['parameter']['column'] = hive_column
    ddl = ddl[:-1] + ") partitioned by (logdate string) stored as orc location '/ods/{}/{}'".format(self.hive_db, table_name)
    hive_sql.append(ddl)
    hive_sql.append("ALTER TABLE {}.{} ADD IF NOT EXISTS PARTITION (logdate='${{system.biz.curdate}}')".format(self.hive_db, table_name))
    return ";".join(hive_sql)

  def convert_mysql_to_hive_type(self, column: Column) -> str:
      """
      Converts a SQLAlchemy Column object to a Hive compatible data type.

      Args:
          column: A SQLAlchemy Column object.

      Returns:
          The corresponding Hive data type string.
      """
      # Mapping of SQLAlchemy types to Hive types
      type_map = {
          String: "string",
          Integer: "int",
          BigInteger: "bigint",
          Float: "double",
          Numeric: "decimal",
          Date: "date",
          DateTime: "timestamp"
      }
      d = str(column.type).lower()
      if d.startswith("varchar") or d.endswith("text"):
        return "string"
      if d.startswith("bit"):
        return "boolean"
      if d.startswith("tinyint"):
        return "int"
      if d.startswith("datetime"):
        return "string"
      if d.startswith("integer"):
        return "int"
      return d

  def create_ds_process(self, table_name: str):
    # 0. get 2 genCode
    url = self.ds_url + "/task-definition/gen-task-codes?genNum=2"
    resp = requests.get(url, headers=headers)
    if resp.json()['code'] != 0:
      print("get genCode failed")
      return
    codes = resp.json()['data']
    # 1. get hive create table DDL
    ddl = self.generate_hive_table_ddl(table_name)
    # 2. create add partition sql
    payload = {
        "name": "etl_{}_{}".format(self.db, table_name),
        "executionType": "PARALLEL",
        "taskDefinitionJson": json.dumps([
          {
            "delayTime": 0,
            "description": "",
            "environmentCode": -1,
            "failRetryInterval": 1,
            "failRetryTimes": 0,
            "flag": 1,
            "isCache": 0,
            "name": "create_tbl_and_part",
            "taskParams": {
              "localParams": [],
              "resourceList": [],
              "hiveCliTaskExecutionType": "SCRIPT",
              "hiveSqlScript": ddl,
            },
            "taskPriority": "MEDIUM",
            "taskType": "HIVECLI",
            "timeout": 0,
            "timeoutFlag": 0,
            "timeoutNotifyStrategy": "",
            "workerGroup": "default",
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH",
            "code": codes[0],
          },
          {
            "code": codes[1],
            "delayTime": 0,
            "description": "",
            "environmentCode": -1,
            "failRetryInterval": 1,
            "failRetryTimes": 0,
            "flag": 1,
            "isCache": 0,
            "name": "import data",
            "taskParams": {
              "localParams": [],
              "resourceList": [],
              "customConfig": 1,
              "json": json.dumps(self.addax_mysql2hdfs_template),
              "xms": 32,
              "xmx": 64
            },
            "taskPriority": "MEDIUM",
            "taskType": "ADDAX",
            "timeout": 0,
            "timeoutFlag": 0,
            "timeoutNotifyStrategy": "",
            "workerGroup": "default",
            "cpuQuota": -1,
            "memoryMax": -1,
            "taskExecuteType": "BATCH"
          }
        ]),
        "taskRelationJson": json.dumps([
          {
            "name": "",
            "preTaskCode": 0,
            "preTaskVersion": 0,
            "postTaskCode": codes[0],
            "postTaskVersion": 0,
            "conditionType": "NONE",
            "conditionParams": {}
          },
          {
            "name": "",
            "preTaskCode": codes[0],
            "preTaskVersion": 0,
            "postTaskCode": codes[1],
            "postTaskVersion": 0,
            "conditionType": "NONE",
            "conditionParams": {}
          }
        ]),
        "releaseState": "ONLINE"
    }
    resp = requests.post(self.ds_url + "/process-definition", data=payload, headers=headers, verify=False)
    if resp.json()['code'] == 0:
      return True
    else:
      print(resp.json())
      return False

def create_cmdline() -> ArgumentParser:
  parser = ArgumentParser(description="Create Hive table from MySQL")
  parser.add_argument("--host", type=str, required=True, help="MySQL host")
  parser.add_argument("--port", type=int, required=True, help="MySQL port")
  parser.add_argument("--user", type=str, required=True, help="MySQL user")
  parser.add_argument("--password", type=str, required=True, help="MySQL password")
  parser.add_argument("--db", type=str, required=True, help="MySQL database")
  parser.add_argument("--hive_db", type=str, required=True, help="Hive database")
  return parser

if __name__ == "__main__":
    #db2Hive = DB2Hive(107190654829696, "wgzhao-pc", 3306, "dbquery", "9P5V1n3LQC3p", "data_query", "odsquery")
    db2Hive = DB2Hive(107190654829696, "188.175.3.75", 33060, "root", None, "zkeco", "odszk")
    for tbl in db2Hive.get_all_tables():
        if not db2Hive.create_ds_process(tbl):
          break
