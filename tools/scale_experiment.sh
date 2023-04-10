
cd ../build/
mkdir results
cat /mnt/ssd1/kron_17_stream_binary > /dev/null

num_forwarders=10

for machines in {16..24..8}
do
	procs=$((2*num_forwarders + 1 + machines))
	wprocs=$machines
	echo $wprocs
	cat /proc/net/dev > results/kron_17_stream_np${wprocs}
	mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 file 3 /mnt/ssd1/kron_17_stream_binary results/kron_17_stream_np${wprocs}
	cat /proc/net/dev >> results/kron_17_stream_np${wprocs}
done
cd -
