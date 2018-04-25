
#python adjust.py weit.dev zj.dev.predict > adjust.dev


#../liblinear-1.96/train -c 0.1 -s 2 adjust.dev

#../liblinear-1.96/predict zj.dev adjust.dev.model adjust.dev.predict
#../liblinear-1.96/predict zj.test adjust.dev.model adjust.test.predict

#python acc.py zj.dev zj.dev.predict
python earn.py count weit.dev zj.dev.predict | python curve.py curve.dev
#python acc.py zj.test zj.test.predict
python earn.py count weit.test zj.test.predict | python curve.py curve.test
sz curve.dev
sz curve.test

python earn.py count weit.txt weit.txt.predict | python curve.py curve.svm
sz curve.svm


