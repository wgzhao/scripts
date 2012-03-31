#!/usr/bin/python -tt
#-*- coding:utf-8 -*-
__Author__ = 'wgzhao, wgzhao##gmail.com'
__URL__ = 'http://blog.wgzhao.com/2010/05/28/core-python-programming-2nd-chapter-18-exercise.html'
'''
Core Python Programming 2nd exercise
chapter 18
'''
import os
import threading
from time import time,ctime,sleep
import re
from Queue import Queue
from random import randint,random

class MyThread(threading.Thread):
  def __init__(self,func,args,name=''):
    threading.Thread.__init__(self)
    self.name = name
    self.func = func
    self.args = args

  def getResult(self):
    return self.res

  def run(self):
    self.res = apply(self.func,self.args)
    


def mtfind(filename,patt,nseek):
    '''
    在文件给定的范围内查找特定字符出现的次数
    '''
    f = open(filename,'r')
    f.seek(nseek)
    data = f.read(size)
    result = patt.findall(data)
    return len(result)

def ex18_4(filename):
    '''
    查找给定文件中特定字符出现的次数
    '''
    size=20480 #number of reading file
    threads = []
    patt = re.compile(r'if')
    #set the number of threads by file's size and reading size
    stat = os.stat(filename)
    nthreads = int(round(float(stat.st_size) / size + 0.5))
    for i in range(nthreads):
        nseek = i * size
        t = MyThread(mtfind,(filename,patt,nseek),mtfind.__name__)
        threads.append(t)
    for i in range(nthreads):
        threads[i].start()

    count =0

    for i in range(nthreads):
        threads[i].join()
        count += threads[i].getResult()

    print count


def urlcvt(data,ptuple):
    '''
    replace email/url into linked schema 
    data string :original string
    ptuple tuple: set of patterns,form of tuple
    @return string
    '''
    for p in ptuple:
        data=p.sub(r"\1",data)
   
    return data
    

def ex18_5(filename):
    '''
    将某文件里的URL或者email地址转换为有链接的地址
    采用线程方式
    '''
    rows=100 #rows
    patterns  =( re.compile(r'(\b[\w-]*?@[\w.-]*?\.[\w]*)\b'), re.compile(r'(\b[\w-]*://.[\w.-]*\b)'))
    threads = []
    #set the number of threads by file's size and reading size
    data = open(filename,'r').readlines()
    nthreads = int(round(len(data) / float(rows) + 0.5))
    
    for i in range(nthreads):
        
        t = MyThread(urlcvt,(''.join(data[i * rows : (i+1) * rows]),patterns),urlcvt.__name__)
        threads.append(t)
        
    for i in range(nthreads):
        threads[i].start()

    result = []
    for i in range(nthreads):
        threads[i].join()
        result.append(threads[i].getResult())
    
    
    f = open(filename + '.html','w')
    f.write(''.join(result))
    f.close()
    
    


def writeQ(queue):
    '''
    producter
    '''
    num = randint(4,10)
    print 'producting %d object(s) for Q...' % num
    for i in range(num):
        queue.put('xxx',1)
        sleep(random())
    print 'size now',queue.qsize()
    
def readQ(queue):
    '''
    消费者
    '''
    num = randint(3,8)
    print 'consumed %d object(s) from Q...' % num
    for i in range(num):
        queue.get(1)
        sleep(random())
    print 'size now',queue.qsize()
    
        
def ex18_8():
    '''
    生产者与消费者问题
    生产者和消费者个数随机
    每次生产或者消费的数量随机
    '''
    nwriter = randint(3,10) #number of producter
    nconsumer = randint(2,7) #number of nconsumer
    q = Queue(80) #queue size
    threads = []
    print 'producter number:',nwriter
    print 'consumer number:',nconsumer
    
    for i in range(nwriter):
        t = MyThread(writeQ,(q,),writeQ.__name__)
        threads.append(t)
        
    for i in range(nconsumer):
        t = MyThread(readQ,(q,),readQ.__name__)
        threads.append(t)
        
    print 'begin NOW at:',ctime(time())
    for t in threads:
        t.start()    
    for t in threads:
        t.join()
        
    print 'all DONE at:',ctime(time())
