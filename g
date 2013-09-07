#!/bin/bash
## remote login haodou beijing/tianjin machine(s)
bj="211.151.130.203"
tj="123.150.200.203"
if [ $# -lt 1 ];then
	#no arguments,login beijing haodou80 node default
	ssh weiguo@${bj}
fi
if [ $# -ge 1 ];then
	if [ "$1" != "tj" -a "$1" != "bj" ];then
		echo "the first argument must be tj or bj"
		exit 65
	fi
	
	cmd="ssh -t weiguo@${!1}"
	if [ -n $2 ];then
		## $2 must be two digital
		if [[ $2 == *[!0-9]* ]];then
			echo "the second argumen must be digital string"
			exit 65
		fi
		cmd="${cmd} /home/weiguo/bin/g $2"
	fi
	eval $cmd
fi
exit 0
