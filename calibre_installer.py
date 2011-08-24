#!/usr/bin/python -tt
# -*- coding:utf-8 -*-

import sys, os, shutil, subprocess, urllib, re, stat, platform, time, signal

MOBILEREAD='https://dev.mobileread.com/dist/kovid/calibre/'

def is64bit():
    return platform.architecture()[0] == '64bit'

def get_url(fname):
    return 'http://status.calibre-ebook.com/dist/linux'+('32' if 'i686' in fname
            else '64')
    return 'http://sourceforge.net/projects/calibre/files/%s/'+fname % sys.argv[1]
    return MOBILEREAD+fname

def download_tarball():
    fname = 'calibre-%s-i686.tar.bz2' % sys.argv[1] #version e.g  0.7.1
    if is64bit():
        fname = fname.replace('i686', 'x86_64')
    print 'Will download and install', fname
    #fname, headers = urllib.urlretrieve(get_url(fname),
    #        reporthook=Reporter(fname))
    #using multiple threads
    strcmd ='axel -n 20  --output=%s %s' %(fname, get_url(fname))
    #print "%s" % str(cmd)
    os.system(strcmd)
    print 'Downloaded', os.stat(fname).st_size, 'bytes'
    print 'Checking downloaded file integrity...'
    subprocess.check_call(['tar', 'tjf', fname], stdout=open('/dev/null', 'w'),
            preexec_fn=lambda:
                        signal.signal(signal.SIGPIPE, signal.SIG_DFL))
    return fname

def extract_tarball(tar, destdir):
    print 'Extracting application files...'
    if hasattr(tar, 'read'):
        tar = tar.name
    subprocess.check_call(['tar', 'xjf', tar, '-C', destdir], stdout=open('/dev/null', 'w'),
            preexec_fn=lambda:
                        signal.signal(signal.SIGPIPE, signal.SIG_DFL))

def download_and_extract(destdir):
    try:
        f = download_tarball()
    except:
        print 'Failed to download, retrying in 30 seconds...'
        time.sleep(30)
        try:
            f = download_tarball()
        except:
            print 'Failed to download, aborting'
            sys.exit(1)

    if os.path.exists(destdir):
        shutil.rmtree(destdir)
    os.makedirs(destdir)

    print 'Extracting files to %s ...'%destdir
    extract_tarball(f, destdir)

def main():
    defdir = '/opt'
    destdir = raw_input('Enter the installation directory for calibre [%s]: '%defdir).strip()
    if not destdir:
        destdir = defdir
    destdir = os.path.abspath(destdir)
    if destdir == '/usr/bin':
        print destdir, 'is not a valid install location. Choose',
        print 'a location like /opt or /usr/local'
        return 1
    destdir = os.path.join(destdir, 'calibre')
    if os.path.exists(destdir):
        if not os.path.isdir(destdir):
            print destdir, 'exists and is not a directory. Choose a location like /opt or /usr/local'
            return 1

    download_and_extract(destdir)

    mh = os.path.join(destdir, 'calibre-mount-helper')
    if os.geteuid() == 0:
        os.chown(mh, 0, 0)
        os.chmod(mh,
            stat.S_ISUID|stat.S_ISGID|stat.S_IRUSR|stat.S_IWUSR|stat.S_IXUSR|stat.S_IXGRP|stat.S_IXOTH)
    else:
        print 'WARNING: Not running as root. Cannot install mount helper.',
        print 'Device automounting may not work.'

    pi = os.path.join(destdir, 'calibre_postinstall')
    subprocess.call(pi, shell=True)
    print
    print 'Run "calibre" to start calibre'
    return 0


if __name__ == '__main__':
  if os.getuid() != 0:
    print 'must be root'
    sys.exit(2)
  if len(sys.argv) < 2:
    print sys.argv[0],' <verion>'
    print 'Usage',sys.argv[0],' 0.7.1.'
    sys.exit(1)
  main()
