#!/bin/bash
# convert all app log archivers to lzop compress
if [ -z "$1" ];then
    matchdir="/user/yarn/logs/source-log.php.CDA39907*/201[34]*/*.tar.gz"
else
    matchdir="$1"
fi

allfiles=$(hdfs dfs -ls  $matchdir 2>/dev/null |awk '/\.gz/ {print $NF}')
for f in $allfiles
do
    filename=$(basename $f)
    filedir=$(dirname $f)
    #get file
    echo "get $filename"
    hdfs dfs -get $f .
    #uncompress
    gunzip $filename
    #compress with lzop,delete origin file after success
    lzop -U ${filename%\.gz}
    #put newfile to same path
    hdfs dfs -moveFromLocal ${filename%\.gz}.lzo $filedir
    #create index 
    hadoop jar /usr/lib/hadoop/lib/hadoop-lzo-0.6.0.jar com.hadoop.compression.lzo.LzoIndexer ${filedir}
    #remove old file
    hdfs dfs -rm $f
done

    
