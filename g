#!/bin/bash
## remote login haodou beijing/tianjin machine(s)
bj="211.151.130.203"
tj="123.150.200.203"
if [ $# -lt 1 ];then
	#no arguments,login beijing haodou80 node default
	ssh weiguo@${bj}
fi
if [ $# -ge 1 ];then
	cmd="ssh -t weiguo@${!1}"
	if [ -n $2 ];then
		cmd="${cmd} /home/weiguo/bin/g $2"
	fi
	eval $cmd
fi
exit 0
