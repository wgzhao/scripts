function dim(){
	python infoRate2.py $1 $2 $3 $4 > $1.$2; sz $1.$2; rm $1.$2
}

function rate(){
	dim $1 avg $2 $3
	dim $1 sum $2 $3
	dim $1 csum $2 $3
	dim $1 num $2 $3
	dim $1 exceptRate $2 $3
	dim $1 entropy $2 $3
}

rate $1 $2 $3

