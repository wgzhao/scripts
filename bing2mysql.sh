#!/bin/bash
## location: 10.1.1.10 dest=/data/crontab,owner=weiguo:sysadmin,mode=755
## description:
# 从Hive中定时导出数据分析平台的中间数据到MySQL数据库，用于展现
tbl_search_re='rpt_app_*|rpt_grp_*|rpt_haodou_*|rpt_hoto_*|rpt_rcp_*'
function usage()
{
        echo "$0 [ -t <table lists,separator by comma> ] [-l ]  "
        echo """
        -t the lists of table will dump,separator by comma ,e.g 'spt_app_index,spt_rpt_dm'
        -l list all tables will dump then exit
        -h print the help
                """
        exit 2
}

function list_tables() 
{
    tbls=$(hive -S --database bing -e "show tables like '${tbl_search_re}'" 2>/dev/null)
    for tbl in ${tbls[@]}
    do
        echo $tbl
    done
    exit 3
}


while getopts "t:lh" opt
do
        case $opt in
        t) tbls=$(echo $OPTARG |tr ',' ' ')
        ;;
        l) list_tables
        ;;
        h) usage
        ;;
        esac
done
if [ -z "$tbls" ];then
    #get all tables from hive database
    tbls=$(hive -S --database bing -e "set hive.cli.print.header=false;show tables like '${tbl_search_re}'" 2>/dev/null)
fi
dbname="bing"
mycmd="mysql -N --raw --batch   --disable-pager "
mysql_dsn="-uimp -h10.1.1.50 -pimp123"
hivemeta_dsn="-uro -h10.1.1.12 -pro123"
RETVAL=0
for tbl in ${tbls[@]}
do
    #import to mysql
    #删除MySQL已经存在的表，以防止数据结构有所改变 
	mysql $mysql_dsn ${dbname} -e "drop table if exists ${dbname}.${tbl};" 2>/dev/null

    #create it
    #从Hive的show create table中提出建表语句，但是难以获得分区字段，废弃
    #tblschema=$(hive -S -e "show create table ${dbname}.${tbl}" 2>/dev/null |sed -n '/^CREATE/,/)$/p' |sed 's/\bstring\b/varchar(512)/g' |sed 's/CREATE EXTERNAL TABLE/CREATE TABLE/')
	
    #从hive元数据中（这里是MySQL）获得表的字段信息，并进行替换和拼接 
	schema="create table ${dbname}.${tbl} ("
	while read col
	do 
		schema="$schema ${col},"
	done < <($mycmd $hivemeta_dsn  hive  -e "select concat(c.column_name,' ',(case when c.type_name = 'string' then 'varchar(512)' else c.type_name end)) as col from DBS d join TBLS t   on d.db_id=t.db_id join SDS s   on t.sd_id=s.sd_id join COLUMNS_V2 c   on s.cd_id=c.cd_id   where d.name='${dbname}' and t.tbl_name = '${tbl}' order by c.integer_idx asc;" 2>/dev/null )
	
	#除了正规字段外，因为不想在MySQL中创建分区表，因此把Hive中分区字段也纳入到表字段中
	while read pkey 
	do
		schema="$schema ${pkey},"
	done < <($mycmd $hivemeta_dsn  hive -e "select concat(p.pkey_name,' ',(case when p.pkey_type = 'string' then 'varchar(512)' else p.pkey_type end)) as col from DBS d join TBLS t   on d.db_id=t.db_id join SDS s   on t.sd_id=s.sd_id join PARTITION_KEYS p on p.tbl_id = t.tbl_id  where d.name='${dbname}' and t.tbl_name = '${tbl}' order by p.integer_idx asc;" 2>/dev/null)
	
	#拼接建表语句组后一部分，注意要删除多余的,
	schema="${schema%?}) ENGINE=InnoDB DEFAULT CHARSET=utf8;"
	
    mysql $mysql_dsn $dbname -e "$schema" 2>/dev/null
    if [ $? -ne 0 ];then
        echo "execute DDL failed: $schema"
        RETVAL=2
        continue
    fi
    
    #export to file from hive 
    #hive -S -e "select * from ${dbname}.${tbl}" 2>/tmp/hive_export_${tbl}.log >${tbl}.dat
    hdfs dfs -text "/bing/data/${tbl}/*/*" 2>/tmp/hive_export_${tbl}.log >${tbl}.dat
    if [ $? -ne 0 ];then
        echo "maybe the table ${tbl} has not partition,directly export"
        hdfs dfs -text "/bing/data/${tbl}/*" 2>/tmp/hive_export_${tbl}.log >${tbl}.dat
        if [ $? -ne 0 ];then
            echo "failed to export ${dbname}.${tbl} from Hive"
            RETVAL=1
            continue
        fi
    fi
    
    mysqlimport $mysql_dsn --fields-terminated-by='\t' --delete --local $dbname ${tbl}.dat 2>/dev/null
    if [ $? -ne 0 ];then
        echo "failed to import MySQL ${dbname}.${tbl} "
        RETVAL=3
    else
        rm -f ${tbl}.dat
    fi
    
    rm -f /tmp/hive_export_${tbl}.log
    
done
exit $RETVAL
