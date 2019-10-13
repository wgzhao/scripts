#!/usr/bin/python -tt
# -*- coding:utf-8 -*-
__DOC__ = '''
    identifies duplicate files within given directories including subdirectories
     pydupes.py uses files' size and md5sums to find
     duplicate files within a set of directories.
    '''
__Author__ = 'wgzhao,wgzhao##gmail.com'

import os
from hashlib import md5
from sys import exit, argv

class diskwalk(object):
    """API for getting directory walking collections"""
    def __init__(self, path):
        self.path = path
    def enumeratePaths(self):
        """Returns the path to all the files in a directory as a list"""
        path_collection = []
        for dirpath, dirnames, filenames in os.walk(self.path):
            for file in filenames:
                fullpath = os.path.join(dirpath, file)
                path_collection.append(fullpath)
        return path_collection

    def enumerateFiles(self):
        """Returns all the files in a directory as a list"""
        file_collection = []
        for dirpath, dirnames, filenames in os.walk(self.path):
            for file in filenames:
                file_collection.append(file)
        return file_collection

    def enumerateDir(self):
        """Returns all the directories in a directory as a list"""
        dir_collection = []
        for dirpath, dirnames, filenames in os.walk(self.path):
            for dir in dirnames:
                dir_collection.append(dir)
        return dir_collection
    
def create_checksum(path):
    """
    Reads in file. Creates checksum of file line by line.
    Returns complete checksum total for file.
    """
    fp = open(path)
    checksum = md5()
    while True:
        buffer = fp.read(8192)
        if not buffer:
            break
        checksum.update(buffer)
    fp.close()
    checksum = checksum.digest()
    return checksum
    


def findDupes(path = '/tmp'):
    dup = {}
    record = {}
    d = diskwalk(path)
    files = d.enumeratePaths()
    for file in files:
        compound_key = (os.path.getsize(file),create_checksum(file))
        if compound_key in record:
            if compound_key in dup:
                dup[compound_key].append(record[compound_key])
            else:
                dup[compound_key] = [ record[compound_key] ]
            dup[compound_key].append(file)
        else:
            #print "Creating compound key record:", compound_key
            record[compound_key] = file
    return dup

if __name__ == "__main__":
    
    if len(argv) < 2:
        try:
            path = input('pls input dir:')
        except:
            path = ''
    else:
        path = argv[1]
    if not path: exit(1)
    
    dupes = findDupes(path)
    
    for k,v in list(dupes.items()):
        print()
        for filename in v:
            print(filename)