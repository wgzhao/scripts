if [ -z $1 ]; then
	echo "no tag!!"
	exit 1
fi
cat user.tag | grep $1 | awk '{print $1}' | sort -u | grep -v haodou | awk '{if(length($1) > 10) print $0}'

