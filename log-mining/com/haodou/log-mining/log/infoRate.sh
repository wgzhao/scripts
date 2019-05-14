function dim(){
	python infoRate.py $1 $2 $3 $4 > $1.$2; sz $1.$2; rm $1.$2
}

function rate(){
	dim $1 avg $2 $3
	dim $1 sum $2 $3
	dim $1 csum $2 $3
	dim $1 num $2 $3
	dim $1 cavg $2 $3
}

rate $1 $2 $3

