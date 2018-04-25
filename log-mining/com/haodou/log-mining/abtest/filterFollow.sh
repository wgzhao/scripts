if [ -z $1 ] ;then
	AB="A"
else
	AB=$1
fi

if [ -z $2 ]; then
	followType="info"
else
	followType=$2
fi

if [ $followType == "info" ]; then
	echo "info-followType:"$followType
	python filterAB.py abUser.txt $AB $(python readConf.py startTime)  | python followMethod.py f1 info.getinfo#info.getlastestinfo rid
else
	echo "like-followType:"$followType
	python filterAB.py abUser.txt $AB $(python readConf.py startTime)  | python followMethod.py likehead like.add id
fi

