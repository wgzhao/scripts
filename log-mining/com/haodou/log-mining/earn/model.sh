
#head -200000 zj3.txt | tail -40000 | python earn.py Curve > curve.all
#cat curve.all | python curve.py curve.zj
#sz curve.zj

#div=67770
#for((i=$div;i< 340000;i+=$div))

#	do
#		head -$i zj4.txt | tail -$div | python model.py
#	done

#echo "-------------------------------"
#head -160000 zj4.txt | python model.py nn
#head -215000 zj4.txt |  tail -55000 | python model.py Curve | python curve.py phase.dev
#tail -92900 zj4.txt | python model.py Curve | python curve.py phase.test
#echo "-------------------------------"

cat zj4.txt | python model.py Curve | python curve.py curve.zj

if [ ! -z $1]; then
	sz phase.dev
	sz phase.test
	sz curve.zj
fi

