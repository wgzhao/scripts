#!/usr/bin/env python3
# location: hive@infa01:/opt/infalog/bin
"""
日志展示程序，和报表系统配合，用于在页面上获取调度日志的内容，便于快速定位错误信息
run: gunicorn -b 0.0.0.0:5001 -w 4 -D fsbrowser:app
"""

import concurrent.futures
import hashlib
from importlib.resources import path
import os
import re
import sys
from datetime import datetime, timedelta
from functools import cmp_to_key
from typing import List, Union
import subprocess
import uvicorn
from fastapi import FastAPI, Request, status, HTTPException
from fastapi.responses import HTMLResponse


app = FastAPI(
    title="日志文件流程接口",
    description="用来获取、展示采集服务推送的所产生的日志内容",
    docs_url="/docs",
    debug=True
)

infa_cmd_whitelist = []

if sys.platform == 'darwin':
    PROD = False
    LOGDIR = '/tmp/log'
    app.debug = True
else:
    PROD = True
    LOGDIR = '/opt/infalog/log'
    app.debug = False

def cmp_file(x, y):
    r_x = re.sub(r'.*(\d{8})_(\d{6}).*.log', r'\1\2', x)
    r_y = re.sub(r'.*(\d{8})_(\d{6}).*.log', r'\1\2', y)
    return int(r_y) - int(r_x)


def find_files(folder: str = ".", pattern: re.Pattern = None) -> List[str]:
    """
    查找指定目录下的匹配文件名的所有文件

    :param folder str 要查找的文件目录，相对 LOGDIR 而言
    :param pattern re.Pattern 文件匹配模式，这是一个正则表达式

    :returns 符合要求的文件列表
    """
    res = []
    os.chdir(LOGDIR)
    if not os.path.exists(folder):
        return res
    for f in os.scandir(folder):
        if pattern.match(f.name):
            res.append(folder + '/' + f.name)
    return res


@app.get("/fsapi", response_class=HTMLResponse, tags=["default"])
async def index():
    usage = '''<h3>
    <pre>
    get specify hdfs file's content
    url: /fs/cdate/kind/fname
    e.g: /fs/20181024/hadoop_proc/sp_report_vw
    it convert kind and fname to upper, and take the fname as wildcase to match all filename
    </pre>
        </h3>
        '''
    return usage


@app.get("/fsapi/get", response_class=HTMLResponse)
def get_content(fname: str):
    """
    get the fname's content and convert all carrie lines to html br elements
    """
    try:
        # return open(fname, 'r').read().replace('\n','<br/>')
        return "<pre>" + open(LOGDIR + '/' + fname, 'r').read() + "</pre>"
    except Exception as e:
        return f"failed to read {fname}: {e}\n"


def _get_links(items: List[str], host: str) -> str:
    outstr = []
    template = "<a href='/fsapi/get?fname={}'>{}</a>"

    for item in sorted(items, key=cmp_to_key(cmp_file)):
        outstr.append(template.format(item, os.path.basename(item)))

    return "<p>" + '<br/>\n'.join(outstr) + "</p>"


@app.get("/fsapi/fs/{cdate}/{job}/", response_class=HTMLResponse)
def getfs(request: Request, cdate: str, job: str) -> Union[str, List[str]]:
    """
    获取指定日期和任务名称下的日志文件（一般是多个文件)
    @params:
        cdate: string of date separated by comma, e.g 2020102,20200103
        job: string   job's name , e.g hadoop_SP_ALLBRANCH_99
    """
    # split date
    d1, d2 = cdate.split(',')
    btime = datetime.strptime(d1, '%Y%m%d')
    etime = datetime.strptime(d2, '%Y%m%d')
    # get all matched filenames
    # 日志需要从三个地方查询 @issue-6
    # 日志参数有变更 #6
    curtime = btime
    threads: List[concurrent.futures.Future] = []
    result = []
    pattern = None
    today = datetime.now().strftime('%Y%m%d')
    with concurrent.futures.ThreadPoolExecutor(max_workers=10) as executor:
        while curtime <= etime:
            cur_str = curtime.strftime("%Y%m%d")
            if job.startswith('tuna'):
                pattern = re.compile(r'{}_{}_.*.log'.format(job, cur_str))
            else:
                pattern = re.compile(r'tuna_.*?_{}_\d+_{}_.*.log'.format(job, cur_str))
            if (cur_str >= today):
                threads.append(executor.submit(find_files, ".", pattern))
            else:
                threads.append(executor.submit(find_files, cur_str, pattern))
            curtime += timedelta(days=1)

        for thread in threads:
            result.extend(thread.result())

    if not result:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="没有找到任何日志文件"
        )

    if len(result) == 1:
        return get_content(result[0])
    else:
        return _get_links(result, request.base_url.hostname)

@app.get("/fsapi/ds")
def execute_ds(ctype: str, sp_id: str = None) -> bool:
    """
    根据传递过来的参数执行特定的ds调度任务
    """
    real_command = "/opt/infalog/bin/sp_alone.sh start_wkf "

    if ctype == "source":
        real_command += " soutab_start"
    elif ctype == "sp":
        real_command += "sp_start"
    elif ctype == "spcom":
        # need sp_id
        if not sp_id:
            raise HTTPException(
                status_code=status.HTTP_400_BAD_REQUEST,
                detail="sp_id 不能为空"
            )
        real_command += " spcom " + sp_id
    else:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail="请勿做无谓的尝试"
        )
    ret = subprocess.getoutput(real_command)
    return ret
    
if __name__ == '__main__':
    log_config = uvicorn.config.LOGGING_CONFIG
    log_config["formatters"]["access"]["fmt"] = "%(asctime)s - %(levelname)s - %(message)s"
    uvicorn.run('fsbrowser:app', host='0.0.0.0', port=5001, reload=True, workers=4, log_config=log_config)
