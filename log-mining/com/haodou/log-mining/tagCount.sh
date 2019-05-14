
cd util
source ./hadoop.sh
cd ..

inputDir=/backup/CDA39907/001/2015-*/*
outputDir=/user/zhangzhonghui/logcount/tmp
files=column.py,tagCount.py,util/cateidName.txt
mapper=python,tagCount.py,cateidName.txt
reducer=python,tagCount.py,reduce

basic $inputDir $outputDir $files $mapper $reducer


