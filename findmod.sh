#!/bin/bash
#MODULEMAP=/usr/share/frexhwd/modules.fhm
MODULEMAP=/lib/modules/`uname -r`/modules.pcimap

# findmodule [vendor] [device] [subvendor] [subdevice]
findmodule() {
	#first actually match
	ret=`grep "0x0000$1 0x0000$2" $MODULEMAP`
	
	if [ $? -eq 0 ];then
		#cat $ret |awk '{print $1}'
		echo $ret |awk '{print $1}'
	else
		echo ""
	fi
}

#for DEVINFO in /sys/bus/pci/devices/*;do
#  VENDOR=`cat $DEVINFO/vendor`
#  VENDOR=${VENDOR:2}
#  DEVICE=`cat $DEVINFO/device`
#  DEVICE=${DEVICE:2}
#  SUBVENDOR=`cat $DEVINFO/vendor`
#  SUBVENDOR=${SUBVENDOR:2}
#  SUBDEVICE=`cat $DEVINFO/subsystem_device`
#  SUBDEVICE=${SUBDEVICE:2}
#  MODULE=`findmodule $VENDOR $DEVICE $SUBVENDOR $SUBDEVICE`
#  if [ x"$MODULE" != x ]; then
#	echo "find module is $MODULE"
##      modprobe -q $MODULE &< /dev/null

#  fi
#done
MODULE=`findmodule $1 $2`
if [ "x$MODULE" != "x" ];then
 echo "ID $1:$2 is $MODULE"
else
 echo "Oops,find nothing!"
fi
