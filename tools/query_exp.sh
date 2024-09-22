
if [[ $# -ne 2 ]]; then
  echo "Invalid arguments. Require result_file, num_workers"
  echo "result_file:    CSV file in which to place results"
  echo "num_workers:    Number of worker machines."
  exit
fi

num_forwarders=10
result_file=$1
procs=$((2*num_forwarders + 1 + $2))

cd ../build/
cat /mnt/ssd1/kron_17_stream_binary > /dev/null

echo "============  QUERY EXPERIMENTS 1  ============="
mpirun -np $procs -hostfile hostfile -rf rankfile ./query_expr 40 60 /mnt/ssd1/kron_17_stream_binary $result_file --repeat 9 --burst 10 50000

echo "============  QUERY EXPERIMENTS 2  ============="
mpirun -np $procs -hostfile hostfile -rf rankfile ./query_expr 40 60 /mnt/ssd1/kron_17_stream_binary $result_file --repeat 9 --burst 10 50000 --point

cd -
