# source inside build folder
./ebpf_redirect_block_load.sh
tc filter del dev ens1f0np0 parent 1:
#tc filter del dev ens1f0np0 ingress
#tc filter del dev ens1f0np0 egress
./server.out ens1f0np0 $1 1 $2 $3 $4
