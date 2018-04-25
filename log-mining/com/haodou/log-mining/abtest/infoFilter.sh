if [ -z $1 ] ;then
	echo "没有指定AB选项！！"
	exit 1
fi
python filterAB.py abUser.txt $1 $(python readConf.py startTime)  | python methodInfo.py

