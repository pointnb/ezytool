#! /bin/bash

if [ $# -eq 0 ]
then
	echo "Usage: ./run_toJson.sh [type]"
	echo "--type: {naver,daily}"
	exit
fi

for i in 2014 2015 2016
do
	for j in {01..12}
	do
		python toJson.py $i'_'$j'.txt' $i'_'$j'.json' $1
		echo "[$i-$j] finished"
	done
done
