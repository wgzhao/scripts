#!/usr/bin/env python3
"""
多线程抓去新发地批发市场价格信息
"""
import os
import sys
from bs4 import BeautifulSoup as bs
import requests
import time
import pandas as pd
import asyncio
import aiohttp

url_base = 'http://www.xinfadi.com.cn/marketanalysis/0/list/{}.shtml'
max_size = 1000
max_concurrency = 50
# 输出文件控制
## 是否包含表头
header = False
## 分隔符
sep = ','

def get_pages(url):
    """
    获取当前信息的页数
    """
    try:
        content = bs(requests.get(url).content, features="lxml")
        nav = content.find('div',{'class':'manu'})
        trail_page = nav.find('a', {'title':'尾页'})
        return int(trail_page['href'].split('/')[-1].split('.')[0])
    except Exception as err:
        print(f"failed to get pages: {err}")
        return False

async def get_table(task_q):
    async with aiohttp.ClientSession() as session:
        while not task_q.empty():
            url, outfile = await task_q.get()
            try:
                async with session.get(url, timeout=10) as resp:
                    assert resp.status == 200
                    content = await resp.read()
                    df = pd.read_html(content, header=0, 
                            attrs = {'class': 'hq_table'})[0].iloc[:,:7]
                    df.to_csv(outfile, index=False, header=header, sep=sep)
            except Exception as err:
                print(f"Error for url {url}: {err}")

async def produce_tasks(n, task_q):
    for i in range(1,n+1):
        if os.path.isfile(f'/tmp/{i}.csv'):
            continue
        await task_q.put((url_base.format(i), f'/tmp/{i}.csv'))

async def run(num_pages):
    task_q = asyncio.Queue(maxsize=max_size)
    task_producer = asyncio.ensure_future(produce_tasks(num_pages, task_q))
    workers = [asyncio.ensure_future(get_table(task_q)) for _ in range(max_concurrency)]
    try:
        await asyncio.wait(workers+ [task_producer])
    except Exception as err:
        print(err.msg)

def main():
    num_pages = get_pages(url_base.format(1))
    if not num_pages:
        sys.exit(1)
    print("start at", time.strftime("%F %T") )
    ioloop = asyncio.get_event_loop()
    ioloop.run_until_complete(asyncio.ensure_future(run(num_pages)))
    print("end at ", time.strftime("%F %T"))

if __name__ == '__main__':
    main()
            

