
if [[ $# -lt 5 || $# -gt 6 ]]; then
  echo "Invalid arguments. Require csv_directory, min_workers, max_workers, increment, repeats"
 	echo "result_file:    Path to file where we write results"
  echo "min_workers:    Minimum number of worker machines."
  echo "max_workers:    Maximum number of worker machines."
  echo "increment:      Amount of workers to jump by for each experiment."
  echo "repeats:        Number of times to repeat the stream."
  echo "metadata:       Optional argument that prints out to the CSV after each run."
  exit
fi

result_file=$1
min_w=$2
max_w=$3
incr=$4
repeats=$5

cd ../build/
cat /mnt/ssd1/kron_17_stream_binary > /dev/null

data_size=$((38408 * repeats))

num_forwarders=10

for (( machines=min_w; machines<=max_w; machines+=incr ))
do
	procs=$((2*num_forwarders + 1 + machines))
	wprocs=$machines
	echo $wprocs
	cat /proc/net/dev > temp_file
	mpirun -np $procs -hostfile hostfile -rf rankfile ./speed_expr 40 file $repeats /mnt/ssd1/kron_17_stream_binary temp_file
	cat /proc/net/dev >> temp_file

	echo -n "$((wprocs * 16)), $wprocs, " >> $result_file
	python3 ../experiment/parser.py $data_size temp_file >> $result_file
	echo "$6" >> $result_file
done
cd -
