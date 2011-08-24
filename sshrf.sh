#!/bin/bash
if [ $# -lt 2 ];then
echo "$0 <ip suffix> <port>"
echo "Usage: $0 81.88 22 ,indicates it will connect 172.16.81.88:22 "
exit 1
else
sudo ssh -f -N  -p 443 -L 8080:172.16.$1:$2  redflag@219.237.229.195
fi
exit 0
