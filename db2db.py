#!/opt/anaconda3/bin/python3
# coding:utf-8
# location: infa01:/opt/infalog/bin
import sys
import os
import time
import json
import signal
from typing import Dict
from pyspark import SparkConf
from pyspark.sql import SparkSession
import yaml


class SparkETL:
    def __init__(self, jsonfile, threads=1, parts=4, props=None):
        if not os.path.exists(jsonfile):
            print("no such file or directory({})".format(jsonfile))
        fcontent = open(jsonfile, 'r').read()
        try:
            job = json.loads(fcontent)
        except json.decoder.JSONDecodeError:
            # try to load with yaml format
            job = yaml.full_load(fcontent)
        except Exception as e:
            print("failed to parser json file:{}".format(e))
            sys.exit(1)

        conf = SparkConf().setAppName('hive2oracle')
        if props:
            conf.setAll(props.items())

        self.spark = SparkSession \
            .builder \
            .master("local[{}]".format(threads)) \
            .appName('hive2oracle') \
            .config(conf=conf) \
            .enableHiveSupport()\
            .getOrCreate()

        self.spark.sparkContext.setLogLevel("WARN")
        self.spark.sparkContext.getConf().setAll(props.items())
        #print(self.spark.sparkContext.getConf().getAll())
        self.src_props: Dict = job['src']
        self.dest_props: Dict = job['dest']
        self.parts = parts
        self.jsonfile = jsonfile
        signal.signal(signal.SIGINT, self.handler)

    def handler(self, signum, frame):
        print("you pressed Ctrl+c")
        self.spark.stop()

    def run(self):
        is_success = 1
        btime = time.time()
        if self.src_props['jdbc'] == 'hive':
            df = self.spark.sql(self.src_props['sql'])
        else:
            self.src_props['url'] = self.src_props['jdbc']
            if self.src_props.get('sql', None):
                self.src_props['query'] = self.src_props['sql']
                del self.src_props['sql']
            del self.src_props['jdbc']
            df = self.spark.read \
                .format('jdbc') \
                .options(**self.src_props) \
                .load()
            print(df.printSchema())
            sys.exit(1)
        # 参数优化
        self.dest_props['batchsize'] = '5000'
        self.dest_props['isolationLevel'] = "NONE"
        if self.dest_props.get("mode", None) == "overwrite":
            self.dest_props["truncate"] = "true"
        # self.dest_props['queryTimeout'] = "500" #seconds
        df.write.jdbc(url=self.dest_props['jdbc'], table=self.dest_props['dbtable'],
                        mode=self.dest_props['mode'], properties=self.dest_props)
        src_nums = df.count()
        takes = round(time.time() - btime, 2)
        print("result[{}] read:{} takes(s): {}".format(
            self.jsonfile, src_nums, takes))
        is_success = 0
        return is_success


def prod_run(jsonfile):
    """
    production env
    """
    spark_conf = {
        'spark.executor.memory': '4g',
        'spark.driver.memory': '2g',
        'spark.executor.cores': '8',
        'spark.sql.shuffle.partitions': '8',
        'spark.sql.autoBroadcastJoinThreshold': '-1',
        'spark.ui.showConsoleProgress': 'true',
        'spark.debug.maxToStringFields': '300',
        'spark.serializer': 'org.apache.spark.serializer.KryoSerializer',
        'spark.jars': '/opt/app/trino/373/plugin/oracle/ojdbc8-19.3.0.0.jar',
        'spark.driver.extraClassPath': '/opt/app/trino/373/plugin/oracle/ojdbc8-19.3.0.0.jar'
	    #'spark.jars':'/opt/spark/jars/ojdbc6-11.2.0.4.jar',
        #'spark.driver.extraClassPath':'/opt/spark/jars/ojdbc6-11.2.0.4.jar',
    }
    sc = SparkETL(jsonfile, threads=4, parts=4, props=spark_conf)
    return sc.run()


def test(jsonfile):
    spark_conf = { 'spark.jars': '/opt/spark/jars/ojdbc6-11.2.0.4.jar'}
    #spark_conf = {}
    sc = SparkETL(jsonfile, threads=1, parts=4, props=spark_conf)
    return sc.run()


if __name__ == '__main__':
    if len(sys.argv) < 2:
        print("{} <json file>".format(sys.argv[0]))
        sys.exit(1)
    sys.exit(prod_run(sys.argv[1]))

