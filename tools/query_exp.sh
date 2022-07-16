
cd ../build/
mkdir results
cat /mnt/ssd1/kron_17_stream_binary > /dev/null

FILE_NAME=results/query_expr_bursts_point

cat /proc/net/dev > $FILE_NAME
mpirun -np 2561 -hostfile ~/hostfile -bind-to none ./query_expr 36 60 /mnt/ssd1/kron_17_stream_binary $FILE_NAME --repeat 9 --burst 10 50000 --point
cat /proc/net/dev >> $FILE_NAME
cd -
