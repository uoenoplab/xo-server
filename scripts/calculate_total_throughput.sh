#!/bin/bash

if [ $# -ne 3 ]; then
	echo "Usage: $0 [gateway hostname] [link speed] [case]"
	exit 1
fi

gateway="$1"
link_speed="$2"
case="$3"

echo "Size" > size.txt
for size in 8 16 32 64 128 256 512 1024 2048 4096; do
	echo "$size" >> size.txt
done

str=""
for version in uniform zipf0.9 zipf1.1; do
	str="$str size.txt"
	echo "${version} " > "${version}.txt"
	for size in 8 16 32 64 128 256 512 1024 2048 4096; do
		ret=`cat ../${link_speed}Gbps/${case}_${version}/${size}kib/wrk_${gateway}.txt | grep "Transfer/sec:" | wc -l`
		if [ $ret -eq 0 ]; then
			echo "Error" >> "${version}.txt"
		else
			cat ../${link_speed}Gbps/${case}_${version}/${size}kib/wrk_${gateway}.txt | grep "Transfer/sec:" | awk '{ if ($2 ~ /GB/) { print $2*8; } else if ($2 ~ /MB/) { print $2/1000*8; } else printf("error\n");}' >> "${version}.txt"
		fi
	done
	str="$str ${version}.txt"
done

paste $str
rm $str
