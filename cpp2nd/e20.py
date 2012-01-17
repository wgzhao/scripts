#!/usr/bin/env python
#coding=utf-8

from BeautifulSoup import BeautifulSoup as bs
from os.path import isdir, exists, dirname, splitext
from os import makedirs, unlink, sep
from string import find, lower, replace
from urlparse import urlparse, urljoin
from urllib2  import urlopen
from Queue import Queue
from sys import argv,exit
import threading
import re

class Retriever(object):    # download Web pages

    def __init__(self, url):
        self.url = url
        self.file = self.filename(url)
        
    def filename(self, url, deffile='index.htm'):
        parsedurl = urlparse(url, 'http:', 0)  # parse path
        path = parsedurl[1] + parsedurl[2]
        ext = splitext(path)
        if ext[1] == '':
            if path[-1] == '/':
                path += deffile
            else:
                path += '/' + deffile
        ldir = dirname(path)    # local directory
        if sep != '/':  # os-indep. path separator
            ldir = replace(ldir, ',', sep)
        if not isdir(ldir):      # create archive dir if nec.
            if exists(ldir): unlink(ldir)
            makedirs(ldir)
        return path

    def download(self):     # download Web page
        try:
            fin = open(self.file,'w')
            retval = fin.write(urlopen(self.url).read())
            fin.close()
        except IOError:
            retval = ('*** ERROR: invalid URL "%s"' % \
                self.url, )
        return retval

    def parseAndGetLinks(self): # pars HTML, save links
        self.parser = bs(open(self.file).read())
        links = []
        for atag in  self.parser('a'):
            try:
                links.append(atag['href'])
            except:
                pass
        return links
        

def getPage():
    while True:
        url = urlqueue.get() #nowaiting
        print '*** got from urlqueue'
        if not url in seen:
            r = Retriever(url)
            r.download()
            print '... add  to filequeue'
            filequeue.put(r.file,1)
            seen.append(url)
        #urlqueue.task_done()
    
def parserPage():
    while True:
        
        filename = filequeue.get()
        print '*** got from filequeue'
        data = open(filename,'r').read()
        p = bs(data)
        for tag in  [ tag for  tag in p('a')  if len(tag.attrs)>0 and 'href' in tag.attrs[0]]:
            eachLink = tag['href']
            if eachLink[:4] != 'http' and find(eachLink, '://') == -1:
                eachLink = urljoin(url, eachLink)
            patt = re.compile(r'(mailto:|javascript:|telnet:|e2dk:|ftp://|thunder:)')
            if patt.search(lower(eachLink)):
                continue
            if eachLink not in seen and find(eachLink, domain) > 0 :
                urlqueue.put(eachLink,1)
                print '... add to urlqueue'
        #filequeue.task_done()

def main():
    threads=[]
    for i in range(2):
        t = threading.Thread(target=getPage)
        threads.append(t)
    for i in range(2):
        t = threading.Thread(target=parserPage)
        threads.append(t)
        
    for t in threads:
        t.setDaemon(True)
        t.start()
    urlqueue.join()
    filequeue.join()
    
if __name__ == '__main__':
    
    if len(argv) > 1:
        url = argv[1]
    else:
        try:
            url = raw_input('Enter starting URL: ')
        except (KeyboardInterrupt, EOFError):
            url = ''
    if not url:
        exit(1)
    seen = []
    urlqueue = Queue(10)
    filequeue = Queue(20)    
    domain = urlparse(url)[1]
    urlqueue.put(url,1)
    main()
