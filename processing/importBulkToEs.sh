#! /bin/bash

for i in 2014 2015
do
	for j in {01..12}
	do
		curl -s -XPOST localhost:9200/_bulk --data-binary @$i'_'$j'.json'
		echo "[$i-$j] finished"
	done
done


