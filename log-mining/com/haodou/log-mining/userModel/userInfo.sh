source ../util/hadoop.sh

UserInfoBaseDir=/user/zhangzhonghui/userModel/userInfo

today=`date -d -1day +%Y-%m-%d`
outputDir=$UserInfoBaseDir/$today
hdfs dfs -rm -r $outputDir
N=7
if [ ! -z $1 ]; then
	inputDir=/backup/CDA39907/001/$1
else
	hdfsFileUtilEcho="import sys\nsys.path.append('../util')\nimport hdfsFile\n"
	inputDir=$(echo -e $hdfsFileUtilEcho"print hdfsFile.getInputStrPlus('$UserInfoBaseDir',N=$N)" | python )	
fi
echo $inputDir

files=../column.py,userInfo.py
mapper="python,userInfo.py,map"
reducer="python,userInfo.py,reduce"
basic  $inputDir $outputDir $files $mapper $reducer ../

