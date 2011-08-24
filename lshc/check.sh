#!/bin/bash
#***************************************************
#Linux System Health Check(SHC) V0.1
# @since Dec,2008
# @author <a href="mailto:wgzhao@redflag-linux.com">mlsx</a>
# @lastchage $Date: 2010-01-15 13:42:16 +0800 (五, 2010-01-15) $
# @version $Id: check.sh 12 2010-01-15 05:42:16Z wgzhao $
#****************************************************

#global variable 
CPULOAD=5 	#CPU load avg
SPMAX=85	#disk space used ,percenter
SPINODE=80	#disk io nodes used,percenter
SWPUSE=30	#swap space used ,percenter
MEMUSE=70 	#used memory space ,percenter
LOGSIZE=512000 #log file size max value,KB 
TMPDIR=`mktemp -d check.XXXXX 2>/dev/null` 
if [ $? -ne 0 ];then
  mkdir check.$$
  TMPDIR=check.$$
fi
COMPRESS=yes #compress check log or not

BOOTUP=color
RES_COL=60
MOVE_TO_COL="echo -en \\033[${RES_COL}G"
SETCOLOR_SUCCESS="echo -en \\033[1;32m"
SETCOLOR_FAILURE="echo -en \\033[1;31m"
SETCOLOR_WARNING="echo -en \\033[1;33m"
SETCOLOR_NORMAL="echo -en \\033[0;39m"

echo_success() {
  [ "$BOOTUP" = "color" ] && $MOVE_TO_COL
  echo -n "[  "
  [ "$BOOTUP" = "color" ] && $SETCOLOR_SUCCESS
  echo -n $"OK"
  [ "$BOOTUP" = "color" ] && $SETCOLOR_NORMAL
  echo -n "  ]"
  echo -ne "\r"
  return 0
}

echo_failure() {
  [ "$BOOTUP" = "color" ] && $MOVE_TO_COL
  echo -n "["
  [ "$BOOTUP" = "color" ] && $SETCOLOR_FAILURE
  echo -n $"FAILED"
  [ "$BOOTUP" = "color" ] && $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 1
}


echo_warning() {
  [ "$BOOTUP" = "color" ] && $MOVE_TO_COL
  echo -n "["
  [ "$BOOTUP" = "color" ] && $SETCOLOR_WARNING
  echo -n $"WARNING"
  [ "$BOOTUP" = "color" ] && $SETCOLOR_NORMAL
  echo -n "]"
  echo -ne "\r"
  return 1
}

#input Y(es) or N(o) 
answer()
{
key="f"
while [ "$key" != "y" ] && [ "$key" != "n" ]
do
	echo -n "(y/n): "
	read key
	key=${key:0:1}
	key=`echo $key |tr 'A-Z' 'a-z'` 
done
}

alert()
{
echo -e "\\033[0;31m $*  \\033[0;39m"
echo $* >>"$TMPDIR/`hostname`_alert.txt"
#read -p "Press any key to continue" -s -n 1 CHAR
#echo "Press ENTER to continue, or CTRL-C to quit"
#read IGNORE
}
#
#check license valid or not
#return 0,1,65 indicates valid,temporary or invalid ,no rfbootchk
#
checkLicense()
{
	#([ -f /usr/bin/rflicchk ]  &&  [  -f /bin/rfbootchk ] ) || fail "3rd disc had not installed yet?"
	#Asianux3.0 has not rflicchk
	#db3.0/4.0 bypass
	oldkern="2.4.9 2.4.20-8 2.4.21-e.38enterprise"
	kern=`uname -r`
	if echo "$oldkern" |grep -q "$kern"
	then
		return 0
	fi	

	[ -f /bin/rfbootchk ] || return 65
	[ -f /etc/.rflicense ] || return 1

	/bin/rfbootchk >/dev/null 2>&1 &
	sleep 3
	ret=`pidof rfbootchk`

	if [ "x$ret" = "x" ];then
		 return 0
	else
		kill -9 $ret >/dev/null 2>&1 
		return 1
	fi
	return 0
}

#
# get all NICs' information
#
getMac()
{
	#the number of NIC less than 10,I think so
	for i in `/sbin/ifconfig |grep 'Link encap' |awk '{print $1}'  |grep -v 'lo' |grep -v '^[a-z]*[0-9]*:'`
	do
		#maybe the message include chinese,so i need pure english environment
		/sbin/ifconfig "$i" >/tmp/$$ 2>/dev/null
		#draw a line
		#echo "-----"
		#get the NIC name and MAC address
		#ret=`grep 'HWaddr' /tmp/$$ |awk '{print "nic:", $1,"\tMac:", $5}'`
		#ret=`grep 'HWaddr' /tmp/$$ |awk '{print "nic:", $1}'`
		#echo -en "$ret \t"
		#get the inet4 ipaddress
		ret=`grep 'inet addr:' /tmp/$$  |awk  '{print $2}' |awk -F: '{print $2}'`
		echo "${i}: $ret"
		#(( $index = index + 1 ))
		#clean something
		rm -f /tmp/$$
	done
	exit 0
}
#check HA License
getHAStat()
{
if [ -d /opt/redflag/hacluster ] ; then
	clpstat  2>/dev/null
elif [ -d /opt/RSIrsf ] ; then
	rfhacli -v list 2>/dev/null 
	rsfcli  -v list 2>/dev/null
else
	echo ""
fi
}

#check disk capacity,alert use when free space less than specified value
check_disk_capacity()
{

#* first check the capacity 
flag=0

while read dev used mp;do
  if [ ${used%\%} -gt $SPMAX ];then
    echo "dev $dev (mount at $mp) is near or at the max capacity!"
	flag=1
  fi
done< <(df -h -P --sync |awk '/^\/dev/ { print $1,$5,$6}')

#above First < is redirection, second is process substitution.
#or df -h xxxx | while statments 


#then check the utilization ratio of inode
while read dev used mp;do
  if [ ${used%\%} -gt $SPMAX ];then
    echo "dev $dev (mount at $mp) is near or at the max capacity!"
	flag=1
  fi
done< <(df -ih -P --sync |awk '/^\/dev/ { print $1,$5,$6}')


#
 if [ $flag -eq 0 ];then
	 echo_success
	 echo ""
	 return 0
 else
	 echo_warning
	 echo ""
	 return 1
 fi

}

#exist huge log file ?
#consider separse file 2008-12-23 15:03:08 
check_log_size()
{
	ret=`find /var/log -type f -size +512000k -printf "%p,%k\n" 2>/dev/null`
	if [ "x$ret" != "x" ];then
		flag=0
		for i in `echo -e $ret`
		do
			fn=`echo $i |cut -d, -f1`
			fs=`echo $i |cut -d, -f2`
			if [ $fs -ge $LOGSIZE ];then
				alert "$fn  > 500M "
				flag=1
			fi
		done
		if [ "$flag" = "1" ];then
			alert "at least one log file's actual size more than 500MB bytes"
			echo_failure
		else
			echo_success
		fi
	else
		echo_success
	fi
	echo ""
}

#-------------------------
#main function 
#------------------------

#who am i 
if [ $UID != 0 ];then
echo "Ennn,you know, give more,do more"
echo "I wanna # prompt not $"
exit 2
fi



#[ "$2" = "-c" ] && COMPRESS=yes
#unset all language environment
unset LANG
unset LANGUAGE
unset LC_ALL

#----------------------------------------------------------
#
#
#System environment check
#
#----------------------------------------------------------
echo -en "\t Initialization"
echo_success
echo ""

#check log file size
echo -en "\t Check log file size "
check_log_size

#--------------------------------------
#
#System Information Collection
#
#--------------------------------------

echo -en "\t Collect necessary information"

hostname=`hostname`
ip=`getMac`

if [ -f /etc/redflag-release ];then
	version=`cat /etc/redflag-release`
elif [ -f /etc/asianux-release ];then
	version=`cat /etc/asianux-release`
else
	version=`cat /etc/issue`	
fi

kernver=`uname -r -m`
cpuload=`cat /proc/loadavg`

memload=`free -m`

#diskuse=`df -hTl `
##get disk inode utiliy Utilization
#diskuse="${diskuse}

#`df -ihTl `"

#diskio=`iostat -d   5 3`

#memio=`vmstat 5 3`

procnum=`ps -e |wc -l`
filenum=`lsof | wc -l`
loginnum=`who |wc -l`
sysdate=`date`
echo_success
echo ""

echo -en "\t Collect HA Cluster status(if exists)"
hafunc="OK"
failover="OK"
appexec="OK"
appsw="OK"
hastat=`getHAStat`
[ "x$hastat" = "x" ] && hasha=0
echo_success
echo ""

echo -en "\t Collect system logs"
boot_error=`cat /var/log/rflogview/boot_errors`
mail_error=`cat /var/log/rflogview/mail_errors`
sys_error=`cat /var/log/rflogview/system_errors`
user_error=`cat /var/log/rflogview/user_errors`
echo_success
echo ""

echo -en "\t Validation system license"
data[0]="VALID"
data[1]="Temporary or INVALID License"
data[65]="No rfbootchk"
checkLicense
ret=$?
license=${data[$ret]}
[ $ret -eq 0 ] && echo_success
if [ $ret -gt 0 ];then
	alert "\n\t\t" $license
	[ $ret -eq 1 ] && echo_warning
	[ $ret -ge 2 ] && echo_failure
fi	
echo ""
echo -en "\t Check disk capacity"

check_disk_capacity 


echo -en "\t Check CPU load average"
#check cpu loadavg greater than 5 or not?
ret=`uptime |cut -d, -f5`
#float number ?
ret=${ret%\.*}
if [ $ret -gt $CPULOAD ];then
	alert "CPUs have little heavy load"
	echo_warning
	echo ""
else
	echo_success
	echo ""
fi

echo -en "\t Check Virutal Memory utilization"
#check VM ,swap = 0 ?
flag=0
totalswap=`cat /proc/meminfo |grep '^SwapTotal' |cut -d: -f2 |sed -e 's/kB$//g'`
if [ $totalswap -eq 0  ];then
	alert "\n\t\t No SWAP, or No actived SWAP !!"
	flag=1
else
	freeswap=`cat /proc/meminfo |grep '^SwapFree' |cut -d: -f2 |sed -e 's/kB$//g'`
	swp=`expr \( $totalswap - $freeswap \) \* 100 / $totalswap`	
 	if [ $swp -ge $SWPUSE ];then
		alert "\n\t\t SWAP Used space  > ${SWPUSE}%"
		flag=1
	fi
fi

#(used mem - buffer - cache ) / total mem
total=`free  |grep 'Mem' |awk '{print $2}'`
fre=`free |head -n 3 |tail -n 1  |awk '{print $4}'`
memuse=`echo "$total $fre" |awk '{print 100 - $2 / $1 * 100}'`
#float number ?
memuse=${memuse%\.*}
if [ $memuse -ge $MEMUSE ];then
	alert "\n\t\t Physics Memory used exceed $MEMUSE"
	flag=1
fi
if [ $flag -gt 0 ];then
	echo_warning
else
	echo_success
fi
echo ""


echo -en "\t Check log rotate "
logrotate /etc/logrotate.conf >/dev/null 2>&1
if [ $? -eq 0 ];then
	logrotate="OK"
	echo_success
	echo ""
else
	logrotate="False"
	alert "\n\t\t Attention: Log rotate failed  "
	echo_failure
	echo ""
fi


#write file
file="$TMPDIR/${hostname}.xml"
exec 6>&1
exec >$file
echo -e "
	<server>
		<hostname>$hostname</hostname>
		<ip>$ip</ip>
		<version>$version</version>
		<license>$license</license>
		<cpuload>$cpuload</cpuload>
		<memload>$memload</memload>
		<booterror name='启动错误'><![CDATA[ $boot_error
		]]></booterror>
		<mailerror name='邮件错误'><![CDATA[ $mail_error
		]]></mailerror>
		<syserror name='系统错误'><![CDATA[ $sys_error
		]]></syserror>
		<usererror name='用户错误'><![CDATA[ $usererror
		]]></usererror>
		<kernver>$kernver</kernver>
		<procnum>$procnum</procnum>
		<filenum>$filenum</filenum>
		<loginnum>$loginnum</loginnum>
		<sysdate>$sysdate</sysdate>
		<hasha>$hasha</hasha>
		<hastat><![CDATA[
		$hastat
		]]></hastat>
	</server>"
#reset stdout
exec 1>&6 6>&-
#collection system log 
#echo -en "\t Collect system logs"
#CMD=`which sysreport 2>/dev/null`
#OPTIONS=" -norpm"
#if [ -z "${CMD}" ];then
	##sysreport does not exists!
	## get errors & warning Logs infomation 
	#LOG="${1}/$hostname.log"
	#echo "System Log information:" >> $LOG 2>/dev/null
	#echo "rflog error log------" >>$LOG 2>/dev/null
	#cat /var/log/rflogview/*errors >> $LOG 2>/dev/null
	#echo "------error messages -----" >>$LOG 2>/dev/null
	#grep -i error /var/log/messages >> $LOG 2>/dev/null
	#echo "------fail messages -----" >>$LOG 2>/dev/null
	#grep -i fail /var/log/messages >> $LOG 2>/dev/null
	#bzip2 $LOG 2>/dev/null
#else
	#$CMD $OPTIONS  >/tmp/$$ 2>/dev/null <<EOF

	#`hostname`
#EOF
	#filepath=`tail /tmp/$$ |grep -i '^Please ' |tail -n 1 | awk '{print $3}'`
##	filepath=`grep -i '^Please ' /tmp/$$ |awk '{print $3}'`
	#mv $filepath "${1}/"
	#rm -f /tmp/$$
	##Asianux 3.0 will delete tarball file automatic
	##filename=`basename $filepath`
	##rm -rf `echo ${filepath%$filename}`
#fi
#echo_success
#echo ""

#------------------------
rpm -q snort >/dev/null 2>&1
[ $? -eq 0 ] && rpm -e snort >/dev/null 2>&1

# collected rflogview info
if [ -d /var/log/rflogview ];then
cp -a /var/log/rflogview/*_errors* $TMPDIR/ # log has rorated 
fi

#compress it or not ?
if [ "$COMPRESS" = "yes" ];then
  tar -czf "`hostname`-check.tar.gz" "$TMPDIR/"
  rm -rf $TMPDIR
  
fi
#-------------------------
echo -e "\t RedFlag System Health Check(RSHC) complete successfully!"
echo "Please send $collfile to your support representative"
exit 0
