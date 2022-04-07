
cd ../build/
cat /mnt/ssd2/kron_17_stream_binary > /dev/null
for machines in {32..46}
do
	procs=$((1 + (machines * 16)))
	wprocs=$(((machines * 16)))
	echo $wprocs, $machines
	mpirun -np $procs -hostfile hostfile ./speed_expr 20 /mnt/ssd2/kron_17_stream_binary results/kron_17_stream_np${procs}
done
cd -
