cd ../build

num_forwarders=10
num_workers=40

for stream in /mnt/ssd1/real_streams/*.data; do
  cat $stream > /dev/null
  procs=$((num_forwarders*2 + 1 + num_workers))
  out=`basename $stream`
  echo $out
  cat /proc/net/dev > speed_result_$out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 file 11 $stream speed_result_$out
  cat /proc/net/dev >> speed_result_$out
done

for stream in /mnt/ssd1/kron_1[3-7]*; do
  cat $stream > /dev/null
  procs=$((num_forwarders*2 + 1 + num_workers))
  out=`basename $stream`
  echo $out
  cat /proc/net/dev > speed_result_$out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 file 11 $stream speed_result_$out
  cat /proc/net/dev >> speed_result_$out
done
procs=$((num_forwarders*2 + 1 + num_workers))
out=erdos_18
cat /proc/net/dev > speed_result_$out
mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 erdos 262144 40000000000 speed_result_$out
cat /proc/net/dev >> speed_result_$out

out=erdos_19
cat /proc/net/dev > speed_result_$out
mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 erdos 524288 40000000000 speed_result_$out
cat /proc/net/dev >> speed_result_$out

out=erdos_20
cat /proc/net/dev > speed_result_$out
mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 erdos 1048576 100000000000 speed_result_$out
cat /proc/net/dev >> speed_result_$out
cd -