#!/bin/bash -x

osd_nodes=("n08" "n09" "n11")
port="8080"

trap terminate INT

function terminate() {
	pkill wrk
	stop_monitoring
	stop_xo
	#rm $tmp_file $tmp_tmpfile
	echo "Terminating..."
	exit 1
}

if [ $# -ne 5 ]; then
	echo "$0 [webserver name] [gateway hostname] [link speed] [experiment name] [n_obj_to_select]"
	exit
fi

technology="$1"
gateway="$2"
link_speed="${3}Gbps"
case="$4"
n_obj_to_select="$5"

base_dir=`pwd`
inputfile_dir="/root/S3-benchmark/scripts/${technology}_obj_list"
num_conns="300"
duration="30s"

source ${base_dir}/../utils.sh

#for size in 8 16 32 64 128 256 512 1024 2048 3072 4096; do
#	mkdir -p "${base_dir}/${link_speed}/${case}_uniform"
#	cat "${inputfile_dir}/${technology}_${size}kb_obj_balanced.txt" | head -n "$n_obj_to_select" > "${base_dir}/${link_speed}/${case}_uniform/obj_list_${size}KiB.txt"
#done

for size in 4096 2048 1024 512 256 128 64 32 16 8; do
	for forwarding_method in "hybrid" "eBPF" "HW-TC"; do
		use_tc=0
		tc_offload=0
		tc_hybrid=0

		if [ "$forwarding_method" == "eBPF" ]; then
			use_tc=0
			tc_offload=0
		elif [ "$forwarding_method" == "HW-TC" ]; then
			use_tc=1
			tc_offload=1
		elif [ "$forwarding_method" == "SW-TC" ]; then
			use_tc=1
			tc_offload=0
		elif [ "$forwarding_method" == "hybrid" ]; then
			use_tc=1
			tc_offload=1
			tc_hybrid=1
		fi

		launch_xo $use_tc $tc_offload $tc_hybrid
		sleep 5
		#for forwarding_method in "eBPF" "HW-TC" "SW-TC"; do
		input_file="${base_dir}/${link_speed}/${case}_uniform/obj_list_${size}KiB.txt"
		output_dir="${base_dir}/${link_speed}/${case}_uniform/${forwarding_method}/${size}kib"
		mkdir -p "$output_dir"
		echo "Running size ${size}kib ($forward_method): ${output_dir}"

		# start monitoring
		start_monitoring "$output_dir"

		# run wrk against gateway on OSD0
		run_wrk "$gateway" "$port" "${input_file}" "$num_conns" "$duration" "$output_dir" 1
		wait_wrk
		stop_monitoring
		stop_xo
		sleep 5
		#mv /root/xo-server/build/n*.txt "${output_dir}/"
	done
	sleep 5
	#mv /root/xo-server/build/n*.txt "${base_dir}/${link_speed}/${case}_uniform/${forwarding_method}/"
done

#rm $tmp_file $tmptmp_file
echo "Done"
