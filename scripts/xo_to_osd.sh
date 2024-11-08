#!/bin/bash -x

implementation="xo"
data_pool="data_pool"

rados -p $data_pool ls | /root/a.out $data_pool | tee >( egrep 'osd.0' > osd.0.txt ) | tee >( egrep 'osd.1' > osd.1.txt ) | tee >( egrep 'osd.2' > osd.2.txt ) | tee >( egrep 'osd.3' > osd.3.txt )

for size in 8 16 32 64 128 256 512 1024 2048 4096; do
	echo "Sorting ${size}kb..."
	min_obj_count=500
	for osd_id in "osd.0" "osd.1" "osd.2"; do
		#cat ${osd_id}.txt | grep "^$((size * 1024))," | awk -F, -v osd_id="${osd_id}" '{printf("%dkb,%s,%d,%s\n", $1/1024, $2, $1, osd_id);}' > ${implementation}_${size}kb_obj_in_${osd_id}.txt
		cat ${osd_id}.txt | grep "^$((size * 1024))," | awk -F, -v osd_id="${osd_id}" '{printf("/%dkb/%s\n", $1/1024, $2);}' > ${implementation}_${size}kb_obj_in_${osd_id}.txt
		obj_count=`cat "${implementation}_${size}kb_obj_in_${osd_id}.txt" | wc -l`
		if [[ $obj_count -lt $min_obj_count ]]; then
			min_obj_count=$obj_count
		fi
	done

	# equal number of obj per osd
	for osd_id in "osd.0" "osd.1" "osd.2"; do
		cat "${implementation}_${size}kb_obj_in_${osd_id}.txt" | head -n $min_obj_count > tmp
		mv tmp "${implementation}_${size}kb_obj_in_${osd_id}.txt"
	done

	cat "${implementation}_${size}kb_obj_in_osd.0.txt" "${implementation}_${size}kb_obj_in_osd.1.txt" "${implementation}_${size}kb_obj_in_osd.2.txt" | shuf | shuf > "${implementation}_${size}kb_obj_balanced.txt"

	echo "Done: listed $min_obj_count objects per OSD"
done

for osd_id in "osd.0" "osd.1" "osd.2" "osd.3"; do
	rm ${osd_id}.txt
done
