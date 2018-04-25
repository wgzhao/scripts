numA=$(awk '{print $1}' abUser.txt | grep A | wc | awk '{print $1}')
numB=$(awk '{print $1}' abUser.txt | grep B | wc | awk '{print $1}')
sh infoRate.sh $1 backup.AB/ methodInfoAB
cat $1.sum | python divAB.py $numA $numB > $1.ab; sz $1.ab

