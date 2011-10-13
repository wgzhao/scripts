#!/bin/bash
#kill oldest  idle connections 
#author wgzhao (wgzhao@kingbase.com.cn)

#connection limit
connlimit=7
#connections will be killed
killconns=5
#kingbase connnection
user="SYSTEM"
pswd="kb613"
servername="192.168.100.103"
dbname="KB613"
querystr="select procpid,current_query from sys_stat_activity order by backend_start asc;"
ISQL="/usr/local/kingbase/bin/isql"
output="/var/tmp/$$.txt"

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

