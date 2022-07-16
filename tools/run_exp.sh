
cd ../build/
mkdir results
cat /mnt/ssd1/kron_17_stream_binary > /dev/null

for machines in {64..80..8}
do
	procs=$((1 + (machines * 16)))
	wprocs=$(((machines * 16)))
	echo $wprocs, $machines
	cat /proc/net/dev > results/kron_17_stream_np${procs}
	mpirun -np $procs -hostfile ~/hostfile ./speed_expr 36 5 /mnt/ssd1/kron_17_stream_binary results/kron_17_stream_np${procs}
	cat /proc/net/dev >> results/kron_17_stream_np${procs}
done
cd -
