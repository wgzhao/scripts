#!/bin/bash
# copy and run this script to the root of the repository directory containing files
# this script attempts to exclude uploading itself explicitly so the script name is important
# Get command line params

REPO_URL="http://localhost:8080/repository/maven-cxzq"
USERNAME="admin"
PASSWORD="password"
CURDIR="."

function print_usage() {
	echo "`basename $0` [-d <directory>] [-r repo url] [-u username] [-p password] [-x proxy]"
	echo -e "\t usage: `basename $0` -r http://localhost:80801/repository/maven-release -u admin -p admin "
	exit 1
}

if [ $# -lt 1 ];then
	print_usage
fi

while getopts ":d:r:u:p:x:" opt; do
	case $opt in
		d) CURDIR="$OPTARG"
		;;
		r) REPO_URL="$OPTARG"
		;;
		u) USERNAME="$OPTARG"
		;;
		p) PASSWORD="$OPTARG"
		;;
		x) PROXY="$OPTARG"
		;;
		*)
		print_usage
		;;
	esac
done

echo "upload all jar files under $CURDIR to ${REPO_URL}"
echo "Press any key to continue...."
read step

for f in $(find $CURDIR -type f -not -path './mavenimport\.sh*' -not -path '*/\.*' \
 -not -path '*/\^archetype\-catalog\.xml*' \
 -not -path '*/\^maven\-metadata\-local*\.xml' \
 -not -path '*/\^maven\-metadata\-deployment*\.xml' | \
 sed "s|^\./||")
 do
	echo -ne  "upload `basename $f` \t"
	if [ -n "$PROXY" ]; then
		curl -k -u "$USERNAME:$PASSWORD" -x $PROXY -X PUT -T $f ${REPO_URL}/${f} 
	else
		curl -k -u "$USERNAME:$PASSWORD" -X PUT -T $f ${REPO_URL}/${f} 
	fi
	if [ $? -ne 0 ];then
		echo "failed"
	else
		echo "success"
	fi
done 

