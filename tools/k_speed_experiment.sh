cd ../build


if [[ $# -ne 4 ]]; then
  echo "Invalid arguments. Require expr_type, result_file, workers, repeats, k"
  echo "result_file:  CSV for results"
  echo "workers:      Number of worker machines."
  echo "repeats:      Number of times to repeat each file stream."
  echo "k:            Number of spanning forests to calculate."
  exit
fi

dataset_sizes=(
  '14'     # ca_citeseer
  '233'    # google
  '3'      # p2p_gnutella
  '2'      # rec_amazon
  '200'    # web_uk
  '151'    # kron_13
  '2403'   # kron_15
  '9608'   # kron_16
  '38408'  # kron_17
  '343322' # erdos18
  '343322' # erdos19
  '858306' # erdos20
)

num_forwarders=10
result_file=$1
num_workers=$2
repeats=$3
k=$4
procs=$((num_forwarders*2 + 1 + num_workers))
echo $num_workers $num_forwarders $procs


for stream in /mnt/ssd1/real_streams/*; do
  cat $stream > /dev/null
  out=`basename $stream`
  cat /proc/net/dev > temp_file
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k file $repeats $stream temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, $k, " >> $result_file
  python3 ../experiment/parser.py $((${dataset_sizes[$d]} * repeats)) temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
done

for stream in /mnt/ssd1/kron_1[3-7]*; do
  cat $stream > /dev/null
  out=`basename $stream`
  cat /proc/net/dev > temp_file
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k file $repeats $stream temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, $k, " >> $result_file
  python3 ../experiment/parser.py $((${dataset_sizes[$d]} * repeats)) temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
done

if [[ k -lt 8 ]]; then
  out=erdos_18
  cat /proc/net/dev > temp_file
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k erdos 262144 40000000000 temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, $k, " >> $result_file
  python3 ../experiment/parser.py ${dataset_sizes[$d]} temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
fi

if [[ k -lt 4 ]]; then
  out=erdos_19
  cat /proc/net/dev > temp_file
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k erdos 524288 40000000000 temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, $k, " >> $result_file
  python3 ../experiment/parser.py ${dataset_sizes[$d]} temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
fi

if [[ k -eq 1 ]]; then
  out=erdos_20
  cat /proc/net/dev > temp_file
  mpirun -np $procs -hostfile hostfile -rf rankfile ./k_speed_expr 40 $k erdos 1048576 100000000000 temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, $k, " >> $result_file
  python3 ../experiment/parser.py ${dataset_sizes[$d]} temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
fi

cd -
