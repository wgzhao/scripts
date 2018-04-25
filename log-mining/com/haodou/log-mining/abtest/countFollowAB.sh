numA=$(awk '{print $1}' abUser.txt | grep A | wc | awk '{print $1}')
numB=$(awk '{print $1}' abUser.txt | grep B | wc | awk '{print $1}')
sh infoRate.sh fm1 backup.AB/ followAB
cat fm1.sum | python divAB.py $numA $numB > fm1.ab; sz fm1.ab
sh infoRate.sh fm1 backup.AB/ followLikeAB
cat fm1.sum | python divAB.py $numA $numB > fm1.likeAB; sz fm1.likeAB

sh infoRate.sh fm2 backup.AB/ followAB
cat fm2.avg | python divAB.py $numA $numB > fm2.ab; sz fm2.ab
sh infoRate.sh fm2 backup.AB/ followLikeAB
cat fm2.avg | python divAB.py $numA $numB > fm2.likeAB;sz fm2.likeAB

