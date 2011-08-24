#!/bin/bash
if [ $# -lt 1 ];then
 echo "`basename $0` <key>"
 exit 1;
fi
for i in $*
do
gpg --keyserver keyserver.ubuntu.com --recv-key $i && gpg --export --armor |sudo apt-key add -
done
