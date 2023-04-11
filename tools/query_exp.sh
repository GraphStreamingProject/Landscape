cd ../build/
mkdir results
cat /mnt/ssd1/kron_17_stream_binary > /dev/null

FILE_NAME=results/query_expr_uniform_global
cat /proc/net/dev > $FILE_NAME
mpirun -np 61 -hostfile hostfile -rf rankfile ./query_expr 40 50 /mnt/ssd1/kron_17_stream_binary $FILE_NAME --repeat 9
cat /proc/net/dev >> $FILE_NAME


FILE_NAME=results/query_expr_bursts_global
cat /proc/net/dev > $FILE_NAME
mpirun -np 61 -hostfile hostfile -rf rankfile ./query_expr 40 60 /mnt/ssd1/kron_17_stream_binary $FILE_NAME --repeat 9 --burst 10 50000
cat /proc/net/dev >> $FILE_NAME


FILE_NAME=results/query_expr_bursts_point
cat /proc/net/dev > $FILE_NAME
mpirun -np 61 -hostfile hostfile -rf rankfile ./query_expr 40 60 /mnt/ssd1/kron_17_stream_binary $FILE_NAME --repeat 9 --burst 10 50000 --point
cat /proc/net/dev >> $FILE_NAME

cd -
