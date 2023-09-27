#!/opt/anaconda3/bin/python
"""
 general ETL process util
"""
import argparse
import sys
import os
import time
import subprocess
import logging
import socket
import psutil
from urllib.parse import urlparse
import psycopg2

# env detect
ip = socket.gethostbyname(socket.gethostname())
if sys.platform == 'darwin':
    # dev env
    PROD = False
    ADDAXDIR = '/opt/addax/current'
    LOGDIR = '/tmp/log'
    PYTHON = '/usr/bin/python3'
    HIVE_DSN = 'jdbc:hive2://127.0.0.1:10000'
    PRESTO_DSN = '--server 10.60.172.154:28080 --catalog hive'
    CK_DSN = ''
else:
    # prod env
    PROD = True
    LOGDIR = '/opt/infalog/log'
    PYTHON = '/opt/anaconda3/bin/python3'
    PRESTO_DSN = '--server etl01:18080 --catalog hive'
    ADDAXDIR = '/opt/infalog/addax'
    HIVE_DSN = 'jdbc:hive2://dn01:2181,nn01:2181,nn02:2181/;serviceDiscoveryMode=zooKeeper;zooKeeperNamespace=hiveserver2'
    CK_DSN = '/usr/bin/clickhouse-client -h infa03 --port 9000 -m -n --user default'

# 基本命令
cmds = {
    'hive': '/bin/hive ',
    'presto': f"/usr/local/bin/trino {PRESTO_DSN} ",
    'trino': f"/usr/local/bin/trino {PRESTO_DSN} ",
    'spark-sql': 'SPARK_MAJOR=2 spark-sql -S --name %(jobname)s ',
    'beeline': "beeline -u '" + HIVE_DSN + "' --fastConnect=true --hiveconf mapreduce.job.name=%(jobname)s ",
    'spark-submit': 'SPARK_MAJOR=2 spark-submit --driver-memory 8g --executor-memory 8g --name %(jobname)s ',
    'addax': '',
    'shell': 'bash ',
    'schema': '/opt/infalog/bin/jdbcutils ',
    'clickhouse': CK_DSN,
    'allsql': '/usr/local/bin/trino --server infa01:18080 --catalog hive --user hive ',
    'alltrino': '/usr/local/bin/trino --server infa01:18080 --catalog hive --user hive ',
}

# define error code

ERROR_CODE = {
    "FILE_NO_EXISTS": 2,
    "PARSER_ERROR": 3,
    "ARGUMENT_ERROR": 5,
    "RUN_ERROR": 7,
    "RUN_TIMEOUT": 9,
    "UNKNOWN": 97,
}


class Engine(object):
    """basic ETL engine class"""

    def __init__(self):
        self.method = None
        self.cmd = None

    def __str__(self):
        return f"{self.method} with command: {self.cmd}"

class AddaxEngine(Engine):
    """Addax engine """

    cmd: str = None

    def __init__(self, filename=None, params=None, jobname="jobName"):
        """
        Args:
            filename(str): the job filepath, it must be exists
            params(list): addax-specified optional params , key-value pair
                e.g logdate=20190802
        """
        super().__init__()
        self.method = 'addax'
        # self.cmd = cmds[self.method]
        _params = '-DjobName={} '.format(jobname)
        if params:
            # concat all param with -D
            # -Da=b -Dc=d
            _params += " -D".join(params)
        self.cmd = "{}/bin/addax.sh -p'{}' {}".format(
            ADDAXDIR, _params, filename)


class SQLEngine(Engine):
    """execute SQL with variable SQL engine on hadoop
    """

    def __init__(self):
        self.method = None
        super().__init__()


class PrestoEngine(SQLEngine):
    def __init__(self, query, params=None):
        super().__init__()
        self.method = 'presto'
        self.cmd = cmds[self.method]

        self.cmd += f" -f {query} "

        if params:
            self.cmd += '--session ' + ' --session '.join(params)


class HiveEngine(SQLEngine):
    def __init__(self, query,):
        super().__init__()


class ClickHouseEngine(SQLEngine):
    def __init__(self, filepath: str):
        super().__init__()
        self.cmd = 'cat {} | {}'.format(filepath, cmds['clickhouse'])


class CrmdbEngine(SQLEngine):
    def __init__(self, filepath: str):
        super().__init__()
        self.cmd = cmds['crmdb'] + ' < ' + filepath


class Tuna:
    """general ETL engine execution utils
    """
    # job name
    jobname = None
    max_exec_time = 72000

    def __init__(self, args, **kwargs):
        # process logger
        if not os.path.exists(args.filename):
            print("no such file or directory: {}".format(args.filename))
            sys.exit(ERROR_CODE['FILE_NO_EXISTS'])
        self.curr_time = time.strftime("%Y%m%d_%H%M%S")
        self.host = socket.gethostname().split('.')[0]
        self.method = args.method
        # create log directory if not exists otherwise ignore it
        os.makedirs(LOGDIR, exist_ok=True)

        # parse filename remove suffix get jobname
        self.jobname = os.path.splitext(os.path.basename(args.filename))[0]
        self.logfile = "{}/tuna_{}_{}_{}_{}_{}.log".format(
            LOGDIR, args.method, self.jobname, self.curr_time,
            self.host, os.getpid())

        logger = logging.getLogger('tuna')
        logger.setLevel(logging.INFO)
        if not args.console:
            fileHandler = logging.FileHandler(self.logfile)
            fileHandler.setLevel(logging.INFO)
            fileHandler.setFormatter(logging.Formatter(
                "%(asctime)s\t%(process)d\t[%(levelname)s] - %(message)s"))
            logger.addHandler(fileHandler)
        consoleHandler = logging.StreamHandler()
        consoleHandler.setLevel(logging.INFO)
        consoleHandler.setFormatter(logging.Formatter(
            "%(asctime)s\t%(process)d\t[%(levelname)s] - %(message)s"))
        logger.addHandler(consoleHandler)
        self.logger = logger
        self.args = args
        self.max_exec_time = self.args.timeout

    def _kill_process(self, proc):
        """
        terminate all child processes and itself
        """
        while proc.poll() is None:
            for child in psutil.Process(proc.pid).children(recursive=True):
                child.kill()
            proc.kill()

        return True

    # 从日志中抓取关键的错误信息，用来发送告警信息
    # 不同的运行方式，提取错误方式不同

    def _scape_error_msg(self):
        if self.method == 'presto':
            match_cmd = "grep -E 'Query .* failed: '  {} |cut -d: -f2-".format(
                self.logfile)
        elif self.method == 'datax':
            match_cmd = 'grep -Eo "具体错误信息为.*" {} |head -n1 | cut -c 8-'.format(
                self.logfile)
        else:
            return None
        return subprocess.getoutput(match_cmd)

    def _exec_cmd(self, cmd):
        self.logger.info(f"excute cmd: {cmd}")
        proc = subprocess.Popen(cmd, shell=True, stdout=subprocess.PIPE,
                                stderr=subprocess.PIPE, close_fds=True,
                                bufsize=-1, universal_newlines=True)
        cur_epoch = 0
        self.logger.info(f"max execution time : {self.max_exec_time}")
        while proc.poll() is None and cur_epoch < self.max_exec_time:
            time.sleep(3)
            cur_epoch += 3

        if cur_epoch >= self.max_exec_time:
            self.logger.error(
                f"the execution time({cur_epoch}s) of the job is too long, try to terminate it")
            # kill all relative processes
            self._kill_process(proc)
            return ERROR_CODE['RUN_TIMEOUT']

        return_code = proc.returncode
        msg, errmsg = proc.communicate()
        if msg:
            self.logger.info(msg)

        if return_code != 0:
            self.logger.error(errmsg)
            self.logger.error("Failed: {}".format(return_code))
            # 获取日志里的错误信息并输出
            print(self._scape_error_msg())
            return ERROR_CODE['RUN_ERROR']
        else:
            self.logger.info("Successfully: {}".format(return_code))
            return 0

    def process_addax(self):
        """
        invoke alibaba addax ETL engine
        """
        addax = AddaxEngine(filename=self.args.filename,
                            params=self.args.param, jobname=self.jobname)
        # job name
        self.logger.info('>>>>>>>>>>>> job content begin \n ' +
                         open(self.args.filename).read() +
                         '<<<<<<<<<<<< job content end')
        self.logger.info(f"begin to invoke addax engine to execute....")
        # append output to logfile
        addax.cmd += f" >> {self.logfile} 2>&1 "
        return self._exec_cmd(addax.cmd)

    def process_catalog(self, sysid: str) -> str:
        """
        从 postgresql 视图中配置文件中提出数据库连接信息
        用户传递 -m db_xxx，xxx 即为需要查询的 sysid
        """
        db_port_map = {'mysql': 3306, 'clickhouse': 8123,
                       'sqlserver': 1443, 'oracle': 1521, 'postgresql': 54321}
        if sysid.startswith('db_'):
            sysid = sysid[3:]
        # connect postgresql stg01 database
        conn = psycopg2.connect("dbname=stg01 user=stg01 password='JFP7J9FWFQNw' host=127.0.0.1")
        cur = conn.cursor()
        cur.execute("select db_conn_cmd from vw_imp_system where sysid = %s", (sysid.upper(),))
        db_conn_cmd = cur.fetchone()
        if not db_conn_cmd:
            print(f"no such database {sysid} found")
            sys.exit(2)
        return  db_conn_cmd[0]

    def exec_sql(self):
        """ Execute SQL in differenet SQL Engine via `-m` parameter

        Returns:
            int: 0 if success otherwise non-zero
        """
        # if the first line of sql is `#---only_print---`
        line = open(self.args.filename).readline()
        if line.startswith('#---only_print---'):
            print(open(self.args.filename).read())
            return 0
        if self.args.method == "clickhouse":
            real_cmd = "cat {} | {}".format(
                self.args.filename, cmds[self.args.method])
        elif self.args.method in cmds:
            real_cmd = cmds[self.args.method] % ({'jobname': self.jobname})
            real_cmd += ' -f {}'.format(self.args.filename)
        else:
            # 尝试从视图中里获取配置信息，并通过命令行执行脚本
            real_cmd = self.process_catalog(self.args.method)
            real_cmd = "cat {} | {} ".format(self.args.filename, real_cmd)

        # process params if present
        if self.args.param:
            if self.args.method in ['hive', 'spark-sql', 'beeline']:
                real_cmd += '--hivevar ' + ' --hivevar '.join(self.args.param)
            elif self.args.method == 'presto':
                real_cmd += '--session ' + ' --session '.join(self.args.param)
            else:
                print(
                    f"the engine {self.args.method} DOES NOT support parameter yet, ignore it")

        self.logger.info(
            f">>>>>>>>>>>> job content begin \n {open(self.args.filename).read()} \n<<<<<<<<<<<< job content end")

        if self.args.init:
            real_cmd += ' -i {}'.format(self.args.init)
        real_cmd += f" >> {self.logfile} 2>&1"
        return self._exec_cmd(real_cmd)

    def get_table_schema(self):
        """
        copy source table schema and insert into destination table
        """
        real_cmd = cmds[self.args.method] + self.args.filename

        return self._exec_cmd(real_cmd)

    def cmd(self):
        real_cmd = cmds[self.args.method] % ({'jobname': self.jobname})
        if self.args.filename:
            self.logger.info(
                f">>>>>>>>>>>> job content begin \n {open(self.args.filename).read()} \n<<<<<<<<<<<< job content end")
            real_cmd += f" {self.args.filename} >> {self.logfile} 2>&1 "
        return self._exec_cmd(real_cmd)

    def run(self):
        if self.args.method == 'addax':
            retcode = self.process_addax()
        elif self.args.method == 'shell':
            retcode = self.cmd()
        elif self.args.method == 'schema':
            retcode = self.get_table_schema()
        else:
            retcode = self.exec_sql()

        return retcode


if __name__ == '__main__':
    parser = argparse.ArgumentParser(
        description='general purposal ETL schedule util.')
    parser.add_argument('-m', '--method', dest='method',
                        help='choose invoke method, addax is the default')
    parser.add_argument('-i', '--init', dest='init',
                        help='Initialization SQL file', required=False)
    parser.add_argument('-f', '--file', dest='filename', help='SQL from files',
                        required=True)
    parser.add_argument('-p', '--param', dest='param', nargs='+',
                        help='Variable substitution to apply to ETL engine')
    parser.add_argument('-t', '--timeout', dest='timeout', type=int,
                        help='max execution time(seconds) of a job'
                        '0 means unlimited. defaults to 72000,', default=72000)
    parser.add_argument('-c', '--console', dest='console', action='store_true',
                        help='Only print message to console, not log to file')
    parser.add_argument('-u', '--user', dest='user',
                        help='user which running job', default='hive')
    args = parser.parse_args()

    cmds['presto'] += " --user {} ".format(args.user)
    tuna = Tuna(args)
    sys.exit(tuna.run())

