cd ../build


if [[ $# -ne 4 ]]; then
  echo "Invalid arguments. Require workers, repeats"
  echo "expr_type:    Either 'full' or 'limited'. How many data points to collect."
  echo "result_file:  CSV file in which to place results"
  echo "workers:      Number of worker machines."
  echo "repeats:      Number of times to repeat each file stream."
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
expr_type=$1
result_file=$2
num_workers=$3
repeats=$4
procs=$((num_forwarders*2 + 1 + num_workers))
echo $num_workers $num_forwarders $procs
d=0

for stream in /mnt/ssd1/real_streams/*; do
  cat $stream > /dev/null
  cat /proc/net/dev > temp_file
  out=`basename $stream`
  echo $out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 file $repeats $stream temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, " >> $result_file
  python3 ../experiment/parser.py $((${dataset_sizes[$d]} * repeats)) temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
done

for stream in /mnt/ssd1/kron_1[3-7]*; do
  cat $stream > /dev/null
  cat /proc/net/dev > temp_file
  out=`basename $stream`
  echo $out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 file $repeats $stream temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, " >> $result_file
  python3 ../experiment/parser.py $((${dataset_sizes[$d]} * repeats)) temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
done

if [ $expr_type == 'full' ]; then
  cat /proc/net/dev > temp_file
  out=erdos_18
  echo $out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 erdos 262144 40000000000 temp_file
  cat /proc/net/dev >> temp_file
  echo -n "$out, " >> $result_file
  python3 ../experiment/parser.py ${dataset_sizes[$d]} temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))

  cat /proc/net/dev > temp_file
  out=erdos_19
  echo $out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 erdos 524288 40000000000 temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, " >> $result_file
  python3 ../experiment/parser.py ${dataset_sizes[$d]} temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))

  cat /proc/net/dev > temp_file
  out=erdos_20
  echo $out
  mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 erdos 1048576 100000000000 temp_file
  cat /proc/net/dev >> temp_file

  echo -n "$out, " >> $result_file
  python3 ../experiment/parser.py ${dataset_sizes[$d]} temp_file >> $result_file
  echo "" >> $result_file
  d=$((d+1))
fi

cd -
