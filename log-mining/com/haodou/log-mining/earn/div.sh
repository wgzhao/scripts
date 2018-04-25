
#head -200000 zj3.txt | tail -40000 | python earn.py Curve > curve.all
#cat curve.all | python curve.py curve.zj
#sz curve.zj

file=zj.m3
#file=IFnextMonth.txt
#file=ZJIF00.TXT
#file='47#IFL8.txt'
type=multi2
#type=zj0616
#type=ZJIF00

div=67770
#div=33885
div=5645
#div=540
add=0
#add=$div
#div=16940
#div=5650
size=$(wc $file | awk '{print $1}')
#s=330135
s=67750
s=0
for((i=$s+$div;i< $size+$add;i+=$div))

	do
		head -$i $file | tail -1
		head -$i $file | tail -$div | python earn.py $type
	done

echo "-------------------------------"
#head -160000 $file | python earn.py $type
#head -215000 $file |  tail -55000 | python earn.py $type Curve | python curve.py phase.dev
#tail -192900 $file | python earn.py $type Curve | python curve.py phase.test
echo "-------------------------------"

#cat $file | python earn.py $type Curve | python curve.py curve.zj

if [ ! -z $1]; then
	sz phase.dev
	sz phase.test
	#sz curve.zj
fi

