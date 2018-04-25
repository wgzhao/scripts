#!/bin/bash
workdir=$(dirname $0)
options="-uhd_backup -h10.0.10.85 -P3306 -pyf1013 "
## use trigger replace merging
mysql $options < $workdir/merge.sql
#dump web database
${workdir}/mysql2hive.py  -H 10.0.10.85 -f ${workdir}/mysql_tables.conf
#mysql $options -P3306 -e "stop slave;"
#${workdir}/mysql2hive.py -P 3306 -d haodou_warehouse -l hd_
#mysql $options -P3306 -e "start slave;"
#${workdir}/mysql2hive.py -P 3307 -d recipe_warehouse -l rcp_
# ${workdir}/mysql2hive.py -P 3308 -d qunachi_warehouse -l qnc_

#clean all java files and hql files
rm -f *.java *.hql
