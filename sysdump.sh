#!/bin/bash
# 2007-5-25 
# last change 2011-8-29

prog=$0;
cat <<EOF
--------------------------------------------------------
$prog can be used to dump system information, including
  - hardware (/etc/sysconfig/hwconf)
  - syslog (/var/log/messages)
  - CPU (/proc/cpuinfo)
  - memory (/proc/meminfo)
  - others
--------------------------------------------------------

EOF

#safely export PATH
export PATH=/bin:/sbin:/usr/bin:/usr/sbin:/usr/local/bin:/usr/local/sbin

# please speak English.
unset LANG

dumpdir=`mktemp -d /var/tmp/XXXXXX` || exit 1
[ -d $dumpdir ]  && rm -rf $dumpdir


mkdir -p $dumpdir/proc

# set -x		
hostname >& $dumpdir/hostname.out

date;hwclock >& $dumpdir/date.out

echo "dumping hardware info..."

cp /proc/cpuinfo $dumpdir/proc/
cp /proc/cmdline $dumpdir/proc/
cp /proc/loadavg $dumpdir/proc/
cp /proc/slabinfo $dumpdir/proc/
cp -a /proc/scsi $dumpdir/proc
cp /proc/meminfo $dumpdir/proc/

cp /proc/pci $dumpdir/proc/ 2>/dev/null
lspci -vv >& $dumpdir/lspci-vv.out

echo "dumping ifconfig info..."
ifconfig >& $dumpdir/ifconfig.out
ifconfig -s >& $dumpdir/ifconfig-s.out
mii-tool >& $dumpdir/mii-tool.out

echo "dumping module info..."
lsmod >& $dumpdir/lsmod.out

echo "dumping device info..."
cp /proc/devices $dumpdir/proc/

echo "dumping interrupt info..."
cp /proc/interrupts $dumpdir/proc/

echo "dumping I/O info..."
cp /proc/iomem $dumpdir/proc/
cp /proc/ioports $dumpdir/proc/

echo "dumping partition info..."
cp /proc/partitions $dumpdir/proc/
df >& $dumpdir/df.out
df -i >& $dumpdir/df-i.out
fdisk -l >& $dumpdir/fdisk.out
sfdisk -s >& $dumpdir/sfdisk.out

echo "dumping net info..."
cp -rf /proc/net $dumpdir/proc/net

echo "dumping uname..."
uname -a >& $dumpdir/uname.out
cp /etc/issue $dumpdir/
cp /etc/hosts $dumpdir/
[ -f  /boot/grub/grub.conf ] && cp /boot/grub/grub.conf $dumpdir/
[ -f /boot/grub/grub.cfg ] && cp /boot/grub/grub.cfg $dumpdir/

echo "dumping installed packages..."
if [ -x /usr/bin/pkginfo ];then  #Rocky OS
  pkginfo -i >& $dumpdir/packages.out
else
  rpm -qa >& $dumpdir/packages.out
fi

echo "dumping syslog..."
top -b -n 1 >& $dumpdir/top.txt
lsof >& $dumpdir/lsof.txt
[ -d /var/spool/cron ] && tar czf $dumpdir/cron.tgz  /var/spool/cron/
lastlog >& /var/log/lastlog.redflag
cat - >/var/tmp/list <<EOF
/var/log/messages
/var/log/messages.1
/var/log/dmesg
/var/log/dmesg.1
/var/log/lastlog.last
/var/log/errors
/var/log/kernel
/var/log/wtmp
/var/log/cron
/var/log/ha-log
/var/log/ha-debug
/var/log/ha.log
/var/log/debug.log
/var/log/corosync.log
EOF
tar zcf $dumpdir/log.tgz --files-from=/var/tmp/list 2>/dev/null
rm -f /var/log/lastlog.last /var/tmp/list


echo "dumping /etc..."
tar zcf $dumpdir/etc.tgz /etc

echo "other stuff..."
free -m > $dumpdir/free.out
cp /root/.bash_history $dumpdir/bash_history
last > $dumpdir/last.out
ps auxw > $dumpdir/ps-auxw.out
ps auxw --forest > $dumpdir/ps-auxw-forest.out
ps -wef >$dumpdir/ps-wef.out
ipcs > $dumpdir/ipcs.out
netstat -ap > $dumpdir/netstat-ap.out

echo "Packaging..."
[ -f sysinfo.tgz ] && rm -f sysinfo.tgz
curdir=`pwd`
cd $dumpdir/

tar zcf $curdir/sysinfo.tgz *
cd $curdir

[ $? -eq 0 ] && rm -rf $dumpdir

echo "-------------------------------------"
echo "Please copy ./sysinfo.tgz "
echo "-------------------------------------"
