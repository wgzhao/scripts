#!/usr/bin/env python
#-*- coding:utf-8 -*-

import urllib2
import re
from sys import argv, exit
def main(url):
    fd = urllib2.urlopen(url)
    data = fd.read()
    fd.close()
    p = re.compile(r'ed2k="(.*|/)"')
    matches = p.findall(data)
    if matches:
        for l in matches:
            print urllib2.unquote(l)

if __name__ == '__main__':
    if len(argv) < 2:
        print 'one argument is necessary'
        exit(2)
    main(argv[1])
