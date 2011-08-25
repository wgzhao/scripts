#!/bin/bash
#
# Description:  Manages a Kingbase Server as an OCF High-Availability
#               resource
#
# Authors:      zhaomin (mzhao@kingbase.com.cn)
# Contrib:      zhaoweiguo (wgzhao@kingbase.com.cn)
#
# Copyright:    2011-2012 kingbase
#
# License:      GNU General Public License (GPL)
#
# OCF parameters:
#  OCF_RESKEY_kbhome -- Path that KingbaseES installed. Default is /usr/local/BaseSoft/KingbaseES6.1.3
#  OCF_RESKEY_sysctl  - Path to sys_ctl. Default ${OCF_RESKEY_kbhome}/bin/sys_ctl
#  OCF_RESKEY_start_opt - Startup options, options passed to kingbase with -o
#  OCF_RESKEY_ctl_opt - Additional options for sys_ctl (-w, -W etc...)
#  OCF_RESKEY_isql   - Path to isql. Default is ${OCF_RESKEY_kbhome}/bin/isql
#  OCF_RESKEY_kbdata - KINGBASE_DATA directory. Default is ${OCF_RESKEY_kbhome}/data
#  OCF_RESKEY_kbdba  - userID that manages DB. Default is kingbase
#  OCF_RESKEY_kbpsw  - password for OCF_RESKEY_kbuser. Default is MANAGER
#  OCF_RESKEY_kbhost - Host/IP Address where KingbaseES is listening. Default is localhost
#  OCF_RESKEY_kbport - Port where KingbaseES is listening. Default is 54321
#  OCF_RESKEY_kbuser - Username for connection DB. Default is SYSTEM
#  OCF_RESKEY_kbpsw  - password for connection DB. Default is MANAGER
#  OCF_RESKEY_kbdb   - database to monitor. Default is TEMPLATE2
#  OCF_RESKEY_logfile - Path to KingbaseES log file. Default ${OCF_RESKEY_kbhome}/log/kingbase-0.log
#  OCF_RESKEY_stop_escalate - Stop waiting time. Default is 30
###############################################################################
# Initialization:

: ${OCF_FUNCTIONS_DIR=${OCF_ROOT}/resource.d/heartbeat}
. ${OCF_FUNCTIONS_DIR}/.ocf-shellfuncs




usage() {
    cat <<EOF
    usage: $0 start|stop|status|monitor|meta-data|validate-all|methods

    $0 manages a Kingbase Server as an HA resource.

        The 'start' operation starts the Kingbase server.
        The 'stop' operation stops the Kingbase server.
        The 'status' operation reports whether the Kingbase is up.
        The 'monitor' operation reports whether the Kingbase is running.
        The 'validate-all' operation reports whether the parameters are valid.
        The 'methods' operation reports on the methods $0 supports.
EOF
  return $OCF_ERR_ARGS
}

meta_data() {
cat <<EOF
<?xml version="1.0"?>
<!DOCTYPE resource-agent SYSTEM "ra-api-1.dtd">
<resource-agent name="kingbase">
<version>1.0</version>
<longdesc lang="en">
Resource script for KingbaseES. It manages a KingbaseES as an HA resource.
</longdesc>
<shortdesc lang="en">Manages a KingbaseES database instance</shortdesc>

<parameters>
<parameter name="kbhome" unique="0" required="1">
<longdesc lang="en">
Path to KingbaseES installed,Default is /opt/BaseSoft/KingbaseES6.1.3
</longdesc>
<shortdesc lang="en">Kingbase installation directory</shortdesc>
<content type="string" default="${OCF_RESKEY_kbhome}" />
</parameter>
<parameter name="sysctl" unique="0" required="0">
<longdesc lang="en">
Path to sys_ctl command.
</longdesc>
<shortdesc lang="en">Path to sys_ctl command </shortdesc>
<content type="string" default="${OCF_RESKEY_sysctl}" />
</parameter>
<parameter name="start_opt" unique="0" required="0">
<longdesc lang="en">
Start options (-o start_opt passed to kingbase). "-i -p 54321" for example.
</longdesc>
<shortdesc lang="en">start_opt</shortdesc>
<content type="string" default="${OCF_RESKEY_start_opt}" />
</parameter>
<parameter name="ctl_opt" unique="0" required="0">
<longdesc lang="en">
Additional sys_ctl options (-w, -W etc..). Default is "-w"
</longdesc>
<shortdesc lang="en">ctl_opt</shortdesc>
<content type="string" default="${OCF_RESKEY_ctl_opt}" />
</parameter>
<parameter name="isql" unique="0" required="0">
<longdesc lang="en">
Path to isql command.
</longdesc>
<shortdesc lang="en">isql</shortdesc>
<content type="string" default="${OCF_RESKEY_isql}" />
</parameter>
<parameter name="kbdata" unique="0" required="0">
<longdesc lang="en">
Path KingbaseES data directory.
</longdesc>
<shortdesc lang="en">kbdata</shortdesc>
<content type="string" default="${OCF_RESKEY_kbdata}" />
</parameter>
<parameter name="kbdba" unique="0" required="0">
<longdesc lang="en">
User that owns KingbaseES.
</longdesc>
<shortdesc lang="en">kbdba</shortdesc>
<content type="string" default="${OCF_RESKEY_kbdba}" />
</parameter>
<parameter name="kbuser" unique="0" required="0">
<longdesc lang="en">
User that connects KingbaseES. Default is SYSTEM.
</longdesc>
<shortdesc lang="en">kbuser</shortdesc>
<content type="string" default="${OCF_RESKEY_kbuser}" />
</parameter>
<parameter name="kbpsw" unique="0" required="0">
<longdesc lang="en">
Password for kbuser. Default is MANAGER
</longdesc>
<shortdesc lang="en">kbpsw</shortdesc>
<content type="string" default="${OCF_RESKEY_kbpsw}" />
</parameter>
<parameter name="kbhost" unique="0" required="0">
<longdesc lang="en">
Hostname/IP Addreess where KingbaseES is listening
</longdesc>
<shortdesc lang="en">kbhost</shortdesc>
<content type="string" default="" />
</parameter>
<parameter name="kbport" unique="0" required="0">
<longdesc lang="en">
Port where KingbaseES is listening
</longdesc>
<shortdesc lang="en">kbport</shortdesc>
<content type="string" default="${OCF_RESKEY_kbport}" />
</parameter>
<parameter name="kbdb" unique="0" required="0">
<longdesc lang="en">
Database that will be used for monitoring.
</longdesc>
<shortdesc lang="en">kbdb</shortdesc>
<content type="string" default="${OCF_RESKEY_kbdb}" />
</parameter>
<parameter name="logfile" unique="0" required="0">
<longdesc lang="en">
Path to KingbaseES server log output file.
</longdesc>
<shortdesc lang="en">logfile</shortdesc>
<content type="string" default="${OCF_RESKEY_logfile}" />
</parameter>
<parameter name="stop_escalate" unique="0" required="0">
<longdesc lang="en">
Number of retries (using -m fast) before resorting to -m immediate
</longdesc>
<shortdesc lang="en">stop escalation</shortdesc>
<content type="string" default="${OCF_RESKEY_stop_escalate}" />
</parameter>
</parameters>
<actions>
<action name="start" timeout="120" />
<action name="stop" timeout="120" />
<action name="status" timeout="60" />
<action name="monitor" depth="0" timeout="30" interval="30"/>
<action name="meta-data" timeout="5" />
<action name="validate-all" timeout="5" />
<action name="methods" timeout="5" />
</actions>
</resource-agent>
EOF
}


#
#   Run the given command in the Resource owner environment...
#
runasowner() {
    ocf_run su - $OCF_RESKEY_kbdba -c "$*"
}

#
# methods: What methods/operations do we support?
#

kingbase_methods() {
  cat <<EOF
    start
    stop
    status
    monitor
    methods
    meta-data
    validate-all
EOF
}


#kingbase_start: Starts Kingbase
kingbase_start() {
    local kingbase_options

    if kingbase_status; then
        ocf_log info "Kingbase is already running. PID=`head -n1 $PIDFILE`"
        return $OCF_SUCCESS
    fi

    # Remove kingbase.pid if it exists
    rm -f $PIDFILE

    # Check if we need to create a log file
    if ! check_log_file $OCF_RESKEY_logfile
    then
        ocf_log err "Kingbase can't write to the log file: $OCF_RESKEY_logfile"
    return $OCF_ERR_GENERIC
    fi

    # Set options passed to sys_ctl
    kingbase_options=" -D $OCF_RESKEY_kbdata -v $OCF_RESKEY_logfile -o $OCF_RESKEY_start_opt"

    if [ -n "$OCF_RESKEY_kbhost" ]; then
    kingbase_options="$kingbase_options -h $OCF_RESKEY_khost"
    fi

    if [ -n "$OCF_RESKEY_kbport" ]; then
    kingbase_options="$kingbase_options -p $OCF_RESKEY_kbport"
    fi

    output=`runasowner "$OCF_RESKEY_sysctl start $OCF_RESKEY_ctl_opt -D $OCF_RESKEY_kbdata -l $OCF_RESKEY_logfile -o "\'$OCF_RESKEY_start_opt\'" " 2>&1`

    if [ $? -eq 0 ]; then
    # Probably started.....
        ocf_log info "Kingbase start command sent."
    else
        ocf_log err "Can't start Kingbase."
        return $OCF_ERR_GENERIC
    fi

    while :
    do
        kingbase_monitor warn
        rc=$?
        if [ $rc -eq 0 ]; then
            break;
        fi
        sleep 1
    ocf_log debug "Kingbase still hasn't started yet. Waiting..."
    done
    ocf_log info "Kingbase is started."

    return $OCF_SUCCESS
}

#kingbase_stop: Stop Kingbase
kingbase_stop() {
    if ! kingbase_status
    then
        #Already stopped
        return $OCF_SUCCESS
    fi

    # Stop Kingbase, do not wait for clients to disconnect
    output=`runasowner "$OCF_RESKEY_sysctl stop -D $OCF_RESKEY_kbdata  -m fast" 2>&1`

    while :
    do
        kingbase_monitor
        rc=$?
        if [ $rc -eq $OCF_NOT_RUNNING ]; then
            # An unnecessary debug log is prevented.
            break;
        fi
    sleep 1
    ocf_log debug "Kingbase still hasn't stopped yet. Waiting..."
    done

    # Remove kingbase.pid if it exists
    rm -f $PIDFILE

    return $OCF_SUCCESS
}

#
# kingbase_status: is Kingbase up?
#

kingbase_status() {
     if [ -f $PIDFILE ]; then
         PID=`head -n 1 $PIDFILE`
         kill -s 0 $PID >/dev/null 2>&1
         return $?
     fi

     # No PID file
     false
}

#
# kingbase_monitor
#

kingbase_monitor() {
     # Set the log level of the error message
    loglevel=${1:-err}

    if ! kingbase_status
    then
    ocf_log info "KingbaseES is down"
    return $OCF_NOT_RUNNING
    fi

    if [ "x" = "x$OCF_RESKEY_kbhost" ]
    then
       output=`runasowner "$OCF_RESKEY_isql -p $OCF_RESKEY_kbport -U $OCF_RESKEY_kbuser -W $OCF_RESKEY_kbpsw -d $OCF_RESKEY_kbdb -c 'select now();'" 2>&1`
    else
       output=`runasowner "$OCF_RESKEY_isql -h $OCF_RESKEY_kbhost -p $OCF_RESKEY_kbport -U $OCF_RESKEY_kbuser -W $OCF_RESKEY_kbpsw -d $OCF_RESKEY_kbdb  -c 'select now();'" 2>&1`
    fi

    rc=$?
    if [ $rc -ne  0 ]
    then
    ocf_log $loglevel "KingbaseES $OCF_RESKEY_kbdb isn't running"
        if [ $rc -eq 1 ]
        then
            ocf_log err "Fatal error(out of memory or file not found, etc.) occurred while executing the isql command."
        elif [ $rc -eq 2 ]
        then
            ocf_log $loglevel "Connection error(connection to the server went bad and the session was not interactive) occurred while executing the isql command."
        elif [ $rc -eq 3 ]
        then
            ocf_log err "Script error(the variable ON_ERROR_STOP was set) occurred while executing the isql command."
        fi
        ocf_log info "isql command output: $output"
    return $OCF_ERR_GENERIC
    fi

    return $OCF_SUCCESS
}

check_binary2() {
    if ! have_binary "$1"; then
        ocf_log err "Setup problem: couldn't find command: $1"
        return 1
    fi
    return 0
}

# Validate most critical parameters
kingbase_validate_all() {
    if ! check_binary2 "$OCF_RESKEY_sysctl" ||
            ! check_binary2 "$OCF_RESKEY_isql" ||
            ! check_binary2 fuser; then
        return $OCF_ERR_INSTALLED
    fi

#    if [ -n "$OCF_RESKEY_config" -a ! -f "$OCF_RESKEY_config" ]; then
#        ocf_log err "the configuration file $OCF_RESKEY_config doesn't exist"
#        return $OCF_ERR_INSTALLED
#    fi

    return $OCF_SUCCESS
}


#
# Check if we need to create a log file
#

check_log_file() {
    if [ ! -f "$1" ]
    then
        touch $1 > /dev/null 2>&1
        chown $OCF_RESKEY_kbdba:`getent passwd $OCF_RESKEY_kbdba | cut -d ":" -f 4` $1
        #chmod u+x $1
    fi

    #Check if $OCF_RESKEY_kbdba can write to the log file
    if ! runasowner "test -w $1"
    then
        return 1
    fi

    return 0
}

#
#   'main' starts here...
#


if [ $# -ne 1 ]
then
    usage
    exit $OCF_ERR_GENERIC
fi

# Defaults
OCF_RESKEY_kbhome_default="/usr/local/BaseSoft/KingbaseES6.1.3"
OCF_RESKEY_start_opt_default=
OCF_RESKEY_ctl_opt_default="-w"
OCF_RESKEY_kbdba_default="kingbase"
OCF_RESKEY_kbhost_default=
OCF_RESKEY_kbport_default="54321"
OCF_RESKEY_config_default=
OCF_RESKEY_kbuser_default="SYSTEM"
OCF_RESKEY_kbpsw_default="MANAGER"
OCF_RESKEY_kbdb_default="TEMPLATE2"
OCF_RESKEY_stop_escalate_default=30

: ${OCF_RESKEY_kbhome=${OCF_RESKEY_kbhome_defualt}}
: ${OCF_RESKEY_sysctl=${OCF_RESKEY_kbhome}/bin/sys_ctl}
: ${OCF_RESKEY_isql=${OCF_RESKEY_kbhome}/bin/isql}
: ${OCF_RESKEY_kbdata=${OCF_RESKEY_kbhome}/data}
: ${OCF_RESKEY_kbdba=${OCF_RESKEY_kbdba_default}}
: ${OCF_RESKEY_kbhost=${OCF_RESKEY_kbhost_default}}
: ${OCF_RESKEY_kbport=${OCF_RESKEY_kbport_default}}
: ${OCF_RESKEY_config=${OCF_RESKEY_config_default}}
: ${OCF_RESKEY_kbuser=${OCF_RESKEY_kbuser_default}}
: ${OCF_RESKEY_kbpsw=${OCF_RESKEY_kbpsw_default}}
: ${OCF_RESKEY_kbdb=${OCF_RESKEY_kbdb_default}}
: ${OCF_RESKEY_logfile=${OCF_RESKEY_kbhome}/log/kingbase-0.log}
: ${OCF_RESKEY_stop_escalate=${OCF_RESKEY_stop_escalate_default}}

PIDFILE=${OCF_RESKEY_kbdata}/kingbase.pid

case "$1" in
    methods)    kingbase_methods
                exit $?;;

    meta-data)  meta_data
                exit $OCF_SUCCESS;;

    validate-all) kingbase_validate_all
                exit $?;;
esac

kingbase_validate_all
rc=$?
if [ $rc -ne 0 ]
then
    case "$1" in
        stop)    exit $OCF_SUCCESS;;
        monitor) exit $OCF_NOT_RUNNING;;
        status)  exit $OCF_NOT_RUNNING;;
        *)       exit $rc;;
    esac
fi

US=`id -u -n`

if [ $US != root -a $US != $OCF_RESKEY_kbdba ]
then
    ocf_log err "$0 must be run as root or $OCF_RESKEY_kbdba"
    exit $OCF_ERR_GENERIC
fi

# What kind of method was invoked?
case "$1" in
    status)     if kingbase_status
                then
                    ocf_log info "Kingbase is up"
                    exit $OCF_SUCCESS
                else
                    ocf_log info "Kingbase is down"
                    exit $OCF_NOT_RUNNING
                fi;;

    monitor)    kingbase_monitor
                exit $?;;

    start)      kingbase_start
                exit $?;;

    stop)       kingbase_stop
                exit $?;;
    *)
                exit $OCF_ERR_UNIMPLEMENTED;;
esac
