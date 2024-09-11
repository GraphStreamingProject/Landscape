
if [[ $# -ne 4 ]]; then
  echo "Invalid arguments. Require min_workers, max_workers, increment, repeats"
  echo "min_workers:   Minimum number of worker machines."
  echo "max_workers:   Maximum number of worker machines."
  echo "increment:     Amount of workers to jump by for each experiment."
  echo "repeats:       Number of times to repeat the stream."
  exit
fi

min_w=$1
max_w=$2
incr=$3
repeats=$4

result_file=../results/scale_experiment.csv

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

	echo -n "$((wprocs * 16)), $wprocs" >> $result_file
	python3 ../experiment/parser.py $data_size temp_file >> $result_file
done
cd -
