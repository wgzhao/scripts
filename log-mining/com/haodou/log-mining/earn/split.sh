
file=vec.txt
wfile=weit.txt
cat ZJIF00.TXT | python zj.py $wfile > $file

head -200000 $file | head -160000 > zj.train
#cat $file > zj.train
head -200000 $file |tail -40000 > zj.dev
tail -119100 $file  > zj.test

head -200000 $wfile | head -160000 > weit.train
head -200000 $wfile |tail -40000 > weit.dev
tail -119100 $wfile  > weit.test

#cp zj.train zj.dev
#cp weit.train weit.dev

~/libsvm/liblinear-1.96/train -c 0.1 -s 2 zj.train
~/libsvm/liblinear-1.96/predict zj.dev zj.train.model zj.dev.predict
~/libsvm/liblinear-1.96/predict zj.test zj.train.model zj.test.predict
~/libsvm/liblinear-1.96/predict weit.txt zj.train.model weit.txt.predict

sh earn.sh


