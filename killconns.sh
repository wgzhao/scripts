#!/bin/bash
#kill oldest  idle connections 
#author wgzhao (wgzhao@kingbase.com.cn)

#connection limit
connlimit=4
#connections will be killed
killconns=4
#kingbase connnection
user="SYSTEM"
pswd="kb613"
servername="192.168.100.103"
dbname="KB613"
ISQL="/usr/local/kingbase/bin/isql"

#DOT NOT EDIT
#using query_start not backend_start, think about LRU 11-10-14 10:12
querystr="select procpid,current_query from sys_stat_activity order by query_start asc;"

output="/var/tmp/$$.txt"
unset LANG
unset LANGUAGE

if [ $UID -eq 0 ];then
	echo "just normal user,we recommends using KingbaseES owner User(e.g kingbase)"
	exit 3
fi
#query current all connections and status
$ISQL  -t -o $output -h "$servername" -U "$user" -W "$pswd" -c "$querystr" "$dbname"
if [ $? -gt 0 ];then
	echo "connection errror"
	exit 2
fi

conns=$(wc -l $output |awk '{print $1}')
if [ $conns -le $connlimit ];then
	echo "it's ok"
	exit 0
fi

#kill some idle connections
export KILLCONNS=$killconns
ids=$(awk -F'|' 'BEGIN{COUNTER=0;ORS=" "}{if ( toupper($2)  ==   " <IDLE>" && COUNTER < ENVIRON["KILLCONNS"] )  {print $1;COUNTER+=1}}' $output)
sql="/var/tmp/killsession.sql"
> $sql
for i in $ids
do
	echo "alter system kill session $i;" >>$sql
done
$ISQL  -t -o $output -h "$servername" -U "$user" -W "$pswd" -f "$sql" "$dbname"
#cleanup
rm -f $output
rm -f $sql

exit $?

