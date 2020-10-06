#!/usr/bin/env python
#-*- coding:utf-8 -*-

import urllib.request, urllib.error, urllib.parse
import re
from sys import argv, exit
def main(url):
    fd = urllib.request.urlopen(url)
    data = fd.read()
    fd.close()
    p = re.compile(r'href="(ed2k://.*|/)"')
    matches = p.findall(data)
    if matches:
        for l in matches:
            print(urllib.parse.unquote(l))

if __name__ == '__main__':
    if len(argv) < 2:
        print('one argument is necessary')
        exit(2)
    main(argv[1])
