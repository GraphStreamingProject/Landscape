cd ../build


if [[ $# -ne 3 ]]; then
  echo "Invalid arguments. Require workers, repeats"
  echo "workers:     Number of worker machines."
  echo "repeats:     Number of times to repeat each file stream."
  echo "k:           Number of spanning forests to calculate."
  exit
fi

num_forwarders=10
num_workers=$1
repeats=$2
k=$3
procs=$((num_forwarders*2 + 1 + num_workers))
echo $num_workers $num_forwarders $procs


for stream in /mnt/ssd1/real_streams/*; do
  cat $stream > /dev/null
  out=`basename $stream`
  echo "40 file $repeats $stream speed_result_$out"
  cat /proc/net/dev > speed_result_$out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k file $repeats $stream speed_result_$out
  cat /proc/net/dev >> speed_result_$out
done

for stream in /mnt/ssd1/kron_1[3-7]*; do
  cat $stream > /dev/null
  out=`basename $stream`
  echo $out
  cat /proc/net/dev > speed_result_$out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k file $repeats $stream speed_result_$out
  cat /proc/net/dev >> speed_result_$out
done
out=erdos_18
cat /proc/net/dev > speed_result_$out
mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k erdos 262144 40000000000 speed_result_$out
cat /proc/net/dev >> speed_result_$out

out=erdos_19
cat /proc/net/dev > speed_result_$out
mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k erdos 524288 40000000000 speed_result_$out
cat /proc/net/dev >> speed_result_$out

out=erdos_20
cat /proc/net/dev > speed_result_$out
mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k erdos 1048576 100000000000 speed_result_$out
cat /proc/net/dev >> speed_result_$out
cd -
