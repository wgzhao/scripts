#!/bin/bash
#
# chkconfig: - 70 08
### BEGIN INIT INFO
# Provides:          kingbase
# Required-Start:    $remote_fs $syslog
# Required-Stop:     $remote_fs $syslog
# Should-Start:      $network $time
# Should-Stop:       $network $time
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Start and stop the KingbaseES database server daemon
# Description:       Controls the main KingbaseES database server daemon "kingbase"
#                    and its control script "sys_ctl".
# Author:            wgzhao,wgzhao@kingbase.com.cn
### END INIT INFO
#

KINGBASE_HOME="/usr/local/BaseSoft/KingbaseES6.1.3"
KINGBASE_DAEMON="${KINGBASE_HOME}/bin/kingbase"
KINGBASE_CTL="${KINGBASE_HOME}/bin/sys_ctl"
KINGBASE_USER="kingbase"
KINGBASE_CONFDIR="${KINGBASE_HOME}/config"
KINGBASE_DATA=`grep -v '(^#|^$)' ${KINGBASE_CONFDIR}/kls.conf  |sed -e 's/^ *//g'  |grep '^data_dir' |cut -d'=' -f2 | tr  -d "['\t ]"`
KINGBASE_CONF="${KINGBASE_DATA}/kingbase.conf"
pidfile="${KINGBASE_DATA}/kingbase.pid"
platform=`uname -s`
status=3
test -x $KINGBASE_DAEMON || exit 1
test -x $KINGBASE_CTL || exit 1

# Source function library.
#. /etc/rc.d/init.d/functions
#-----------------------------------------------

BOOTUP=color
RES_COL=60
MOVE_TO_COL="echo -en \\033[${RES_COL}G"
SETCOLOR_SUCCESS="echo -en \\033[1;32m"
SETCOLOR_FAILURE="echo -en \\033[1;31m"
SETCOLOR_WARNING="echo -en \\033[1;33m"
SETCOLOR_NORMAL="echo -en \\033[0;39m"
LOGLEVEL=1

echo_success() {
  [ "$BOOTUP" = "color" ] && $MOVE_TO_COL
  echo -n "["
  [ "$BOOTUP" = "color" ] && $SETCOLOR_SUCCESS
  echo -n $"  OK  "
  [ "$BOOTUP" = "color" ] && $SETCOLOR_NORMAL
  echo -n "]"
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


# Run some action. Log its output.
action() {
  local STRING rc

  STRING=$1
  echo -n "$STRING "
  shift
  "$@" && echo_success $"$STRING" || echo_failure $"$STRING"
  rc=$?
  echo
  return $rc
}
#---------------------------------------------------------------------------------
SELF=$(cd $(dirname $0); pwd -P)/$(basename $0)


# priority can be overriden and "-s" adds output to stderr
ERR_LOGGER="logger -p daemon.err -t /etc/init.d/kingbase -i"

# Safeguard (relative paths, core dumps..)
cd /
umask 077


## Do some sanity checks before even trying to start kingbase.
sanity_checks() {
  # check for config file
  if [ ! -r $KINGBASE_CONF ]; then
    action $"$0: WARNING: $KINGBASE_CONF cannot be read. "  /bin/false
  fi

  # check for diskspace shortage
  if LC_ALL=C BLOCKSIZE= df -P $KINGBASE_DATA/. | tail -n 1 | awk '{ exit ($4> 102400) }'; then
    action $"$0: ERROR: The partition with $KINGBASE_DATA is too full!"   /bin/false
    exit 1
  fi
}

## Checks if there is a server running and if so if it is accessible.
#
# check_dead also fails if there is a lost kingbase in the process list
#
#return integer,indicate:
#kingbase server status 0:running 1:daemon dead,pid exists 2:daemon running,pid missing 3:stoped
kingbase_status () {
    pid=`pidof $KINGBASE_DAEMON 2>/dev/null`
    if [ -f "$pidfile" ] && [ "x$pid" != "x" ]; then
        status=0 # EXIT_SUCCESS
    elif [ -f "$pidfile" ] && [ "x$pid" = "x" ]; then
        status=1 #daemon dead
    elif [ ! -f "$pidfile" ] && [ "x$pid" != "x" ]; then
        status=2 #pidfile missing
    else
        status=3 #stoped
    fi
    return $status
}

kingbase_start()
{
    sanity_checks
    kingbase_status
    status=$?
    if [ $status -eq 0 ]; then
       action $"KingbaseES server alreay running"   /bin/true
       exit 0
    fi

    if [ $status -eq 1 ]; then
        #remove pidfile
        rm -f $pidfile
    elif [ $status -eq 2 ];then
        #kill kingbase daemon,sys_ctl not works in condition
        echo  "pid file missing, stop kingbase daemon first"
        savge_stop

    fi
    # Start KingbaseES!
    echo -n "Starting KingbaseES database server kingbase"
    su - $KINGBASE_USER -c "$KINGBASE_CTL start -D $KINGBASE_DATA >/dev/null 2>&1"

    for i in 1 2 3 4 5 6 7 8 9 10; do
        sleep 1
        if kingbase_status; then break; fi
        echo -n "."
    done
    if kingbase_status; then
        action  "" /bin/true
        return 0
    else
        action "Starting KingbaseES data serverFailed,please take a look at the kingbase log" /bin/false
        return 1
    fi
    return 0
}

#first,send TERM signal to kingbase daemon ,otherwise
#use kill with -9 signal
#I hope never never invoke it
savge_stop()
{
        pid=`head -n1 $pidfile 2>/dev/null`
        [ -z $pid ] && pid=$(pidof $KINGBASE_DAEMON)
        su - $KINGBASE_USER -c "$KINGBASE_CTL kill TERM $pid 2>/dev/null"
        server_running=1
        for i in 1 2 3 4 5; do
              sleep 1
              if ! kingbase_status; then
                server_running=
                break;
              fi
        done
        if [ -n $server_running]; then
            kill -9 $pid >/dev/null 2>&1
        fi
        return 0
}
kingbase_stop()
{
    kingbase_status
    status=$?
    echo -n "Stopping KingbaseES database server"
    if [ $status -eq 1 ];then
        rm -f $pidfile
    elif  [ $status -eq 2 -o  $status -eq 0 ]; then
      su - $KINGBASE_USER -c "$KINGBASE_CTL stop -D $KINGBASE_DATA -m fast >/dev/null 2>&1" >/dev/null 2>&1
      r=$?
      if [ "$r" -ne 0 ]; then
        savge_stop
      fi
    fi

    if kingbase_status; then
      action $"Please stop KingbaseES database server manually"    /bin/false
      return 1
    else
      action "" /bin/true
      return 0
    fi
    return 0
}

kingbase_reload()
{
    action $"Reloading KingbaseES database server kingbase"  su - $KINGBASE_USER -c "$KINGBASE_CTL  reload -D $KINGBASE_DATA >/dev/null 2>&1"
    return 0
}

status()
{
    local flag=0
    if kingbase_status; then
      action $"Kingbase is running, pid is `pidof $KINGBASE_DAEMON`!" /bin/true
      flag=0
      #log_action_msg "$($KINGBASE_DAEMON --version)"
    elif [ $status -eq 1 ];then
      action  $"kingbase deamon  dead,but pid file exists!" /bin/false
      flag=1
    elif [ $status -eq 2 ];then
      action $"kingbase daemon running, but pid file missing" /bin/true
      flag=0
    else
      action  $"Kingbase is stopped." /bin/true
      flag=0
    fi
    return $flag
}
#
# main()
#

case "${1:-''}" in
  'start')
    kingbase_start
    ;;

  'stop')
    kingbase_stop
    ;;

  'restart')
    set +e
    $SELF stop
    set -e
    $SELF start
    ;;

  'reload'|'force-reload')
     kingbase_reload
    ;;

  'status')
    status
    ;;
  *)
    echo "Usage: $SELF start|stop|restart|reload|force-reload|status"
    exit 2
    ;;
esac

exit $?
