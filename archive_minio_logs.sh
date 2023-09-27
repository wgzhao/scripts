#!/bin/bash
# archive application logs under minio server
# usage: archive_minio_logs.sh  [-b bucket] [ -n namespace] [ -m archive_month] [ -d archive_day]
# define constants and predefined variables
minio_alias=myminio
archive_year=$(date +"%Y")
archive_month=$(date -d "1 month ago" +"%Y%m")


# get the command line options
while getopts "b:n:m:d:h" opt
do
    case $opt in
        b)
            buckets=$OPTARG
            ;;
        n)
            namespaces=$OPTARG
            ;;
        m)
            archive_month=$OPTARG
            ;;
        d)
            archive_day=$OPTARG
            ;;
        h|?)
            echo "Usage: $0 [-b bucket] [-n namespace] [-m archive_month] [-d archive_day]"
            exit 1
            ;;
    esac
done

if [ -z "$buckets" ]; then
    buckets=$(mc ls --no-color -q $minio_alias | awk '{print $NF}' |tr -d '/')
fi

[ -z "$buckets" ] && exit 1
[ -z "$archive_month" ] && exit 2

if [ -z "$namespaces" ]; then
    namespaces=$(mc ls --no-color -q $minio_alias/$bucket |awk '{print $NF}' |tr -d '/')
fi

# ------------ main -------------
for bucket in $buckets
do
   
    [ -z "$namespaces" ] && continue
    for namespace in $namespaces
    do
        apps=$(mc ls --no-color -q $minio_alias/$bucket/$namespace | awk '{print $NF}' |tr -d '/')
        [ -z "$apps" ] && continue
        for app in $apps
        do
            # determine the current app is date ?
            if [ "$app" = "$archive_year" ]; then
                if [ -n "$archive_day" ]; then
                    days=$archive_day
                else
                    days=$(mc ls --no-color -q $minio_alias/$bucket/$namespace/$app | awk '{print $NF}' |tr -d '/')
                fi
                [ -z "$days" ] && continue
                days=$(mc ls --no-color -q $minio_alias/$bucket/$namespace/$app | awk '{print $NF}' |tr -d '/')
                [ -z "$days" ] && continue
                for day in $days
                do
                    # then delete all original files
                    mc cp --recursive --no-color -q  $minio_alias/$bucket/$namespace/$app/$day .
                    # archive the files into a single tar file
                    (cd $day && tar -czf ${app}.tar.gz *.txt && rm -f *.txt)
                    mc rm --force --recursive --no-color -q $minio_alias/$bucket/$namespace/$app/$day
                    # upload it to minio
                    mc cp --recursive --no-color -q $day $minio_alias/$bucket/$namespace/$app
                    rm -rf $day

                done
            else
                if [ -n "$archive_day" ]; then
                    days=$archive_day
                else
                    days=$(mc ls --no-color -q $minio_alias/$bucket/$namespace/$app | awk '{print $NF}' |tr -d '/')
                fi
                [ -z "$days" ] && continue
                for day in $days
                do
                    # get all files in the $day folder, and archive them to a single tar file , upload the current folder
                    # then delete all original files
                    mc cp --recursive --no-color -q  $minio_alias/$bucket/$namespace/$app/$archive_year/$day .
                    # archive the files into a single tar file
                    (cd $day && tar -czf ${app}.tar.gz *.txt && rm -f *.txt)
                    mc rm --force --recursive --no-color -q $minio_alias/$bucket/$namespace/$app/$archive_year/$day
                    # upload it to minio
                    mc cp --recursive --no-color -q $day $minio_alias/$bucket/$namespace/$app/$archive_year
                    rm -rf $day
                done
            fi
        done
    done
done

