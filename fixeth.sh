#!/bin/bash
[ $UID -eq 0 ] || exit 65
RULES=/etc/udev/rules.d/70-persisent-net.rules
#TEMP="SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", ATTR{address}=="54:52:00:26:3c:6d", ATTR{type}=="1", KERNEL=="eth*", NAME="eth2""

export LANG=C
export LANGUAGE=C

while read nic mac;do 
cat >>$RULES  <<MSG
SUBSYSTEM=="net", ACTION=="add", DRIVERS=="?*", ATTR{address}=="${mac}", TTR{type}=="1", KERNEL=="eth*", NAME="${nic}" 
MSG
done< <(/sbin/ifconfig |grep "^eth" |awk '{print $1,$5}')
