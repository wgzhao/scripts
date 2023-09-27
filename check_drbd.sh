#!/bin/bash
# Copyright (c) 2013 Gregory Duchatelet
# Script to handle MooseFS cluster from keepalived.
#
#
# Usage: checkdrbd.sh action
#
# Where action is :
#
# check	: check that the DRBD resource is Primary, Connected and UpToDate
#	and that MFS master is running
#
# backup: set to backup state. Just checking than DRBD is connected and syncing...
# fault	: set to fault state. Killing MFS Master, unmount partition, set the DRBD resource to Secondary
# master: set to master state. Set the DRBD resource to Primary, mount partition, start MFS Master
# 	then invalidate remote DRBD resource
#
# Note: you can use $MAINTENANCE (/etc/keepalived/maintenance) to disable MFS Master checks
# in case of short MFS master maintenance
#

# Usage func :
[ "$1" = "--help" ] && {
    sed -n -e '/^# Usage:/,/^$/ s/^# \?//p' <$0
    exit
}

#
# CONFIG
#
DRBDADM="/sbin/drbdadm"
MFSMASTER="/sbin/mfsmaster"
# DRBD resource
DRBDRESOURCE="mfs"
# local mount point
MOUNTPOINT="/var/lib/mfsmaster"
# warmup delay
MAXWAIT=30

# how to handle potential split-brain
# 0: manual
# 1: invalidate local data
# 2: invalidate remote data
SPLIT_BRAIN_METHOD=0


#
# CONFIG LOGGER
#
# tail -f /var/log/syslog | grep Keep
LOG="logger -t KeepDRBD[$$] -p syslog" # do not use -i
LOGDEBUG="$LOG.debug"
LOGINFO="$LOG.info"
LOGWARN="$LOG.warn"
LOGERR="$LOG.err"
LOGPID=0


check() {
    # check drbd status
    # step 1. check connect status
    $DRBDADM cstate $DRBDRESOURCE |grep  -q 'Connected'
    if [ $? -gt 0 ];then
        return 1
    fi
    # step 2. check disk status
    $DRBDADM dstate $DRBDRESOURCE |grep  -q '^UpToDate/UpToDate'
    if [ $? -gt 0 ];then
        return 2
    fi
    # step 3. check drbd role
    $DRBDADM role $DRBDRESOURCE |grep -q '^Primary'
    if [ $? -eq 0 ];then
        # primary drbd means mfs must be running
        $MFSMASTER status || return 3
    else
        # secondary , mfs master must be NOT running
        $MFSMASTER status && return 4
    fi
    return 0
}

kill_mfs() {
    $LOGDEBUG "Kill mfs"
    # step 1 : invoke systemctl
    systemctl stop moosefs-master
    sleep 2
    # step 2: check mfsmaster
    pids=$(ps -ef | grep mfsmaster | grep -v grep | awk '{print $2}' | tr '\n' ' ')
    if [ -n "$pids" ]; then
        $LOGWARN "KILLING mfsmaster[$pids]"
        $MFSMASTER stop
    fi
    # step 3: again
    pids=$(ps -ef | grep mfsmaster | grep -v grep | awk '{print $2}' | tr '\n' ' ')
    if [ -n "$pids" ]; then
        $LOGWARN "KILLING -9 mfsmaster[$pids]"
        kill -9 $pids
    fi
}

set_backup() {
    kill_mfs
    # We must be sure to be in replication and secondary state
    ensure_drbd_secondary
}

set_drbd_secondary() {
    awk '{print $2}' /etc/mtab | grep -q "^$MOUNTPOINT"
    if [ $? -eq 0 ]; then
        $LOGWARN "Unmounting $MOUNTPOINT ..."
        umount $MOUNTPOINT
    fi

    $LOGDEBUG "Set DRBD to secondary"
    $DRBDADM secondary $DRBDRESOURCE
    if [ $? -gt 0 ]; then
        $LOGWARN "Unable to set $DRBDRESOURCE to secondary state"
        echo LSOF:
        lsof $MOUNTPOINT | while read line; do
            $LOGWARN "lsof: $line"
        done
        return 1
    fi
    return 0
}

ensure_drbd_secondary() {
    if ! is_drbd_secondary; then
        set_drbd_secondary
        return $?
    fi
}

is_drbd_secondary() {
    # If already Secondary and Connected, do nothing ...
    $DRBDADM role $DRBDRESOURCE |  grep -q ^Secondary
    [ $? -eq 0 ] && return 0
    return 1
}


# WARNING set_master is called at keepalived start
# So if already in "good" state we must do nothing :)
set_master() {
    $DRBDADM role $DRBDRESOURCE | grep -q '^Primary'
    if [ $? -gt 0 ]; then
        $LOGDEBUG "Set DRBD to Primary"
        $DRBDADM primary $DRBDRESOURCE
        $DRBDADM role $DRBDRESOURCE | grep -q '^Primary'
        if [ $? -gt 0 ]; then
            $LOGWARN "Need to force PRIMARY ..."
            $DRBDADM -- --overwrite-data-of-peer primary $DRBDRESOURCE
            $DRBDADM role $DRBDRESOURCE | grep -q '^Primary'
            if [ $? -gt 0]; then
                $LOGWARN "Unable to set PRIMARY"
                return 1
            else
                $LOGWARN "Forced to PRIMARY : OK"
            fi
        fi
    fi
    if ! awk '{print $2}' /etc/mtab | grep "^$MOUNTPOINT" >/dev/null; then
        device=$($DRBDADM sh-dev $DRBDRESOURCE)

        $LOGDEBUG "Mount ..."
        #if ! mount -t $FSTYPE $device $MOUNTPOINT
        mount $device $MOUNTPOINT
        if [ $? -gt 0 ]; then
            $LOGERR "Unable to mount $MOUNTPOINT"
            return 1
        fi
    fi

    # Starting MFS master
    if [ $(pidof mfsmaster | wc -w) -gt 0 ]; then
        $LOGWARN "mfsmater already started ? What did I have to do ?"
        $MFSMASTER stop
    fi
    $LOGDEBUG "Starting mfsmaster ..."
    systemctl start moosefs-master
    $LOGDEBUG "Waiting for mfsmaster ..."
    for i in $(seq 1 $MAXWAIT); do
        sleep 1
        $MFSMASTER status
        [ $? -eq 0 ] && return 0
    done
}


check_mfs() {
    if [ -e $MAINTENANCE ]; then
        return 0
    fi

    m=$($MFSMASTER status 2>&1)
    mcode=$?

    # Check MySQL error codes. Not all errors are fatal, like "1023 too many connections"
    if [ $mcode -gt 0 ]; then
        $LOGWARN "[mfsmaster is unavailable] $m"
        return 1
    fi

    return 0
}

cleanup() {
    kill $LOGPID
    #$LOGDEBUG "Cleanup $LOGPID and $LOGFIFO"
    # Workarround to log something if there is something ...
    # logger will wait for stdin if argument is empty
    # and "rm -f $file" will output nothing
    yvain=$(mktemp -u /tmp/$(basename $0)_yvain.XXXXXX)
    rm -f $LOGFIFO 2>$yvain || $LOGDEBUG "Remove $LOGFIFO failed: $(cat $yvain)"
    test -e $yvain && rm -f $yvain
    #wait
}
set_fault() {
	set_backup
	return $?
}

case "$1" in
check)
    check
    exit $?
    ;;
backup)
    $LOGWARN "=> set to backup state <="
    set_backup
    exit $?
    ;;
fault)
    $LOGWARN "=> set to fault state <="
    set_fault
    exit $?
    ;;
master)
    $LOGWARN "=> set to master state <="
    set_master
    exit $?
    ;;
esac
