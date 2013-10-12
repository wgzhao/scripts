#!/usr/bin/env python
##
'''
get all links from file or stdin or url
'''

import urllib2
from sys import argv,exit,stdin
import os

try:
    from BeautifulSoup import BeautifulSoup as bs
except ImportError:
    print "Install BeautifulSoup first"
    exit(2)
    
def get_content():
    if len(argv) < 2:
        return '\n'.join(stdin.readlines())
        
    if os.path.exists(argv[1]):
        print "argument is a file"
        return open(argv[1]).read()
     
    return urllib2.urlopen(argv[1]).read()
    
    
    
if __name__ == '__main__':
    
    try:
        content = get_content()
    except Exception,err:
        print "cat not get anything %s" % err
        exit(1)
    soup = bs(content)
    urls=[]
    for url in soup.findAll('a',href=True):
        if url['href'].startswith('http://'):
            urls.append(url['href'])
    
    #remove duplicate items
    urls = list(set(urls))
    print '\n'.join(urls)