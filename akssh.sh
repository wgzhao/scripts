#!/bin/bash
###################################
# auto update ssh pub key 
# Author: mlsx
# version: 1.0
# limited
# 1) the hostname or ip address must be the last arguments
# 2) the hostname or ip address can not encrypt
# 3) just use RSA algorithm
# @todo
# 1) ip address or hostname has been encrypted
# 2) more encrypted method beside rsa 
# 3) ip address or hostname can give any position
#############################################

KEY=$HOME/.ssh/known_hosts
which ssh-keyscan >/dev/null 2>&1
if [ $? -ne 0 ]; then
echo "command ssh-keyscan not find in current path"
exit 65
fi
SCAN=`which ssh-keyscan`

SSH=`which ssh` #you have not ssh command ?



#obtain two arguments if it has: host and port
#specify port?
port=`echo $* |egrep -o "\-p.\w*.([0-9])*" |awk '{print $2}'`
if [ $? -ne 0 ];then 
port="22" #default ssh port
fi
#get  the last argument ,given it must be the hostame or ipaddress
eval host=\$$#
#cut the username and @ symbol ,if it has
host=${host##*@}
#KEY file has the host's pub key?
#not hash ip address or hostname
curkey=`grep "$host " $KEY`

if [ $? -ne 0 ];then
	#@todo
	#may ip address or hostname has been hashed
	#------------
	
	#invoke ssh command  directly 
	echo "no key"
	#add the new key
	echo "add new key"
	newkey=`$SCAN -t ssh-rsa $host 2>/dev/null`
	echo "$newkey" >> $KEY
	$SSH $*
else
	#compare pub key
	stype=`echo $curkey |awk '{print $2}'` # pub key encrypt type
	newkey=`$SCAN -t $stype $host 2>/dev/null`
	if [ "$curkey" == "$newkey" ];then
		#nothing to do
		echo "key not change"
		$SSH $*
	else
		echo "update key"
		#update pub key
		grep -v "$host " $KEY >/tmp/key.$$
		echo "$newkey" >>/tmp/key.$$
		mv -f /tmp/key.$$ $KEY
		#O.K, go through ssh 
		$SSH  $*
	fi
fi
exit 0

##################################
#Changelog
# 2008.5.29 original version
###################################