#!/bin/bash
PANDOC=$(which pandoc)
[ -x $PANDOC ] || exit 2
if [ $# -lt 1 ];then
		echo "md2pdf <markdown file>"
		exit 65
fi
VERSION=`$PANDOC  -v |head -n1 |awk '{print $2}'`
templatefile=/usr/share/pandoc-$VERSION/templates/default.latex

$PANDOC -f markdown -t latex --template=$templatefile -o $$.tex $1 

[ $? -eq 0 ] || exit 2

xelatex $$.tex >/dev/null 2>&1

mv $$.pdf ${1%.md}.pdf

rm -rf $$.*
