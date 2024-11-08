if [ -z ${osd_nodes+x} ]; then 
	echo "osd node list is unset"
	exit 1
fi

#if [ -z ${gateway+x} ]; then
#	echo "gateway is unset"
#	exit 1
#fi

num_cpus=`nproc`
wrk_pids=()
tcpdump_pid=0

function stop_monitoring {
	local i=0

	for i in `seq 0 $((${#osd_nodes[@]} - 1))`; do
		echo "Stop dstat on ${osd_nodes[i]} ..."
		ssh root@${osd_nodes[i]} "pid=''; pid=\$(ps aux | grep 'python3 /usr/bin/dool' | grep -v 'grep' | awk '{print \$2}'); if [ \"\$pid\" != \"\" ]; then kill \$pid; fi"
	done

	if [ -z ${gateway+x} ]; then
		echo "No dedicated gateway"
	else
		echo "Stop dstat on ${gateway} ..."
		ssh root@${gateway} "pid=''; pid=\$(ps aux | grep 'python3 /usr/bin/dool' | grep -v 'grep' | awk '{print \$2}'); if [ \"\$pid\" != \"\" ]; then kill \$pid; fi"
	fi
	echo "All dstats stopped"
}

function start_monitoring {
	if [ $# -ne 1 ]; then
		echo "Usage: $0 [output dir]"
		exit 1
	fi

	local i=0
	output_dir="$1"
	stop_monitoring

	for i in `seq 0 $((${#osd_nodes[@]} - 1))`; do
		rm -f ${output_dir}/dstat_${osd_nodes[i]}.csv
		echo "Start dstat on ${osd_nodes[i]} ..."
		ssh root@${osd_nodes[i]} "rm -f ${output_dir}/dstat_${osd_nodes[i]}.csv; nohup dool -T --cpu -C total -d -D dm-0,nvme0n1p1 -r --bytes --net -N ens1f0np0 --output ${output_dir}/dstat_${osd_nodes[i]}.csv --noupdate --noheaders > /dev/null 2>&1 &"
	done
	if [ -z ${gateway+x} ]; then
		echo "No dedicated gateway"
	else
		echo "Start dstat on ${gateway} ..."
		ssh root@${gateway} "rm -f ${output_dir}/dstat_${gateway}.csv; nohup dool -T --cpu -C total -d -D dm-0,nvme0n1p1 -r --bytes --net -N ens1f0np0 --output ${output_dir}/dstat_${gateway}.csv --noupdate --noheaders > /dev/null 2>&1 &"
	fi
	echo "All dstats started"
}

function run_wrk {
	if [ $# -ne 7 ]; then
		echo "Usage: $0 [host] [port] [input file] [num conns] [duration] [output dir] [num instances]"
		exit 1
	fi

	export hosts="$1"
	export port="$2"
	export s3_objects_input_file="$3"

	local num_conns="$4"
	local duration="$5"
	local num_instances="$7"
	local num_cores_to_use="$((num_cpus / num_instances))"
	if [ $num_cores_to_use -ge $num_conns ]; then
		num_cores_to_use=$num_conns
	fi

	local first_core="$((${#wrk_pids[@]} * num_cores_to_use))"
	local last_core="$(( (num_cores_to_use) * (${#wrk_pids[@]} + 1) - 1 ))"

	if [[ $wrk_ignore_timer -eq 1 ]]; then
		echo "Ignore timer"
	fi
	echo "Starting wrk on core ${first_core}-${last_core} to $hosts..."
	if [ "$duration" == "0s" ]; then
		duration=""
		export wrk_ignore_timer=1
	else
		duration="-d$duration"
		unset wrk_ignore_timer
	fi
	taskset -c "${first_core}-${last_core}" ./../../wrk --timeout=4s -t${num_conns} -c${num_conns} ${duration} --latency -s ../../scripts/s3.lua https://${hosts}:${port} > $6/wrk_${hosts}.txt 2>&1 &
	#taskset -c "${first_core}-${last_core}" ./../../wrk -t${num_conns} -c${num_conns} ${duration} --latency -s ../../scripts/s3.lua https://${hosts}:${port} > $6/wrk_${hosts}.txt 2>&1 &
	wrk_pids+=($!)
}

function wait_wrk {
	local i=0
	local status=()

	echo "Wait for ${wrk_pids[@]} wrk intsances to complete..."
	for pid in "${wrk_pids[@]}"; do
		wait "${pid}"
		status+=($?)
	done

	for i in "${!status[@]}"; do
		if [ ${status[$i]} -ne 0 ]; then
			echo "ERROR: Wrk $i exited with ${status[$i]}"
			exit 1
		fi
	done
	echo "All wrk instances stopped"
	wrk_pids=()
}

function stop_xo {
	local i=0
	local gateway_osd=0

	if [ -z ${gateway+x} ]; then
		echo "No dedicated gateway"
	else
		ssh root@$gateway "pkill server.out;"
	fi

	for i in `seq 0 $((${#osd_nodes[@]} - 1))`; do
		node="${osd_nodes[i]}"
		ssh root@$node "pkill server.out;"
	done

	echo "All XO servers stopped"
}

function launch_xo {
	if [ $# -ne 3 ]; then
		echo "Usage: $0 [Use TC (0/1)] [TC Offload (0/1)] [TC Hybrid (0/1)]"
		exit 1
	fi

	stop_xo
	local i=0
	local gateway_osd=0
	local node=0
	if [ $3 -eq 1 ]; then
		nproc='$(($(nproc) - 1))'
	else
		nproc='`nproc`'
	fi
	for i in `seq 0 $((${#osd_nodes[@]} - 1))`; do
		node="${osd_nodes[i]}"
		ssh root@$node "cd /root/xo-server/build; nohup ./../reset.sh $nproc $1 $2 $3 > \`hostname\`.txt 2>&1 &"
	done
	if [ -z ${gateway+x} ]; then
		echo "No dedicated gateway"
	else
		ssh root@$gateway "cd /root/xo-server/build; nohup ./../reset.sh $nproc $1 $2 $3 > \`hostname\`.txt 2>&1 &"
	fi
	sleep 1
	echo "All XO servers started"
}

function get_random_filename {
	name="/tmp/$(tr -dc A-Za-z0-9 </dev/urandom | head -c 13).tmp"
	echo $name
}

function stop_tcpdump {
	local gateway_osd=0
	local i=0
	local node=0

	for i in `seq 0 $((${#osd_nodes[@]} - 1))`; do
		node="${osd_nodes[i]}"
		ssh root@$node "pkill -2 tcpdump"
	done

	if [ -z ${gateway+x} ]; then
		echo "No dedicated gateway"
	else
		ssh root@$gateway "pkill -2 tcpdump"
	fi

#	if [ $tcpdump_pid -ne 0 ]; then
#		kill $tcpdump_pid
#		tcpdump_pid=0
#	fi
}

function launch_tcpdump {
	if [ $# -ne 3 ]; then
		echo "Usage: $0 [iface] [snap len] [filepath]"
		exit 1
	fi

	local gateway_osd=0
	local i=0
	local node=0

	for i in `seq 0 $((${#osd_nodes[@]} - 1))`; do
		node="${osd_nodes[i]}"
		ssh root@$node "nohup tcpdump -U -i $1 -w $3/\`hostname\`.pcap -s $2 tcp > /dev/null 2>&1 &"
	done
	if [ -z ${gateway+x} ]; then
		echo "No dedicated gateway"
	else
		ssh root@$gateway "nohup tcpdump -U -i $1 -w $3/\`hostname\`.pcap -s $2 tcp > /dev/null 2>&1 &"
	fi

#	tcpdump -i $1 -w $3/`hostname`.pcap -s $2 tcp > /dev/null 2>&1 &
#	tcpdump_pid=$!
}
