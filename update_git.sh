#!/bin/bash
# update all project with git
export GIT_SSL_NO_VERIFY=true
# realpath=$(readlink -n -f $0)
# workdir=$(dirname $realpath)
workdir=$(cd "$(dirname $0)"; pwd)
RETVAL=0
cd $workdir
# update master repo
git pull
RETVAL=$?
# update all submodule repos
git submodule foreach git pull origin master
(( RETVAL = RETVAL + $? ))
exit $RETVAL
