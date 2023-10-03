
if [[ $# -ne 3 ]]; then
  echo "Invalid arguments. Require node_list, main_node_cpus, distrib_worker_cpus"
  echo "Node list:            A file that contains the cluster DNS addresses with main node first."
  echo "main_node_cpus:       Number of physical CPUs on the main node."
  echo "distrib_worker_cpus:  Number of physical CPUs on the DistributedWorkers."
  exit
fi

input_file=$1
main_cpu=$2
distrib_cpu=$3

echo $input_file
echo $main_cpu
echo $distrib_cpu

num_forwarders=10

first=0 # True
dw_rank=$((2*num_forwarders+1))

while read line; do
  new_ip=$line
  if [[ $first == 0 ]]; then
    echo "Reading main: $line"
    first=1 # False
    echo "[master]" > new_inventory.ini
    echo "$line" >> new_inventory.ini
    echo "[workers]" >> new_inventory.ini

    new_ip=${new_ip//-/.}
    new_ip=${new_ip/ip./}
    new_ip=`expr match $new_ip '\([0-9]*.[0-9]*.[0-9]*.[0-9]*\)'`
    echo "$new_ip slots=$dw_rank max_slots=$dw_rank" > new_hostfile
    echo "rank 0=$new_ip slot=0-$((main_cpu - num_forwarders - 1))" > new_rankfile
    for (( i=0; i<num_forwarders; i++ )); do
      echo "rank $((i+1))=$new_ip slot=$((main_cpu - num_forwarders + i))" >> new_rankfile
    done
    for (( i=0; i<num_forwarders; i++ )); do
      echo "rank $((i+num_forwarders+1))=$new_ip slot=$((main_cpu - num_forwarders + i))" >> new_rankfile
    done
  else
    echo "Reading worker: $line"
    echo "$line" >> new_inventory.ini

    new_ip=${new_ip//-/.}
    new_ip=${new_ip/ip./}
    new_ip=`expr match $new_ip '\([0-9]*.[0-9]*.[0-9]*.[0-9]*\)'`
    echo "$new_ip slots=1" >> new_hostfile
    echo "rank $dw_rank=$new_ip slot=0-$((distrib_cpu-1))" >> new_rankfile
    dw_rank=$((dw_rank+1))
  fi
  echo $line >> tmp
  echo $new_ip >> tmp # add machine's ip address and dns address to temp file
done <$input_file

# Perform keyscan to setup known_hosts
ssh-keyscan -f tmp > ~/.ssh/known_hosts
rm tmp

cat new_inventory.ini
echo -n "Lines: " 
wc -l new_inventory.ini
echo "Is this inventory correct? [y/n]:"
while true; do
  read correct

  if [[ $correct = [yY] ]]; then
    mv new_inventory.ini inventory.ini
    break
  elif [[ $correct = [nN] ]]; then
    echo "Please ensure that the node list is correct and try again"
    rm new_inventory.ini
    exit 1
  else
    echo "Incorrect input. Try again:"
  fi
done

cat new_hostfile
echo -n "Lines: " 
wc -l new_hostfile
echo "Is this hostfile correct? [y/n]:"
while true; do
  read correct

  if [[ $correct = [yY] ]]; then
    mv new_hostfile hostfile
    mv new_rankfile rankfile
    break
  elif [[ $correct = [nN] ]]; then
    echo "Please ensure that the node list is correct and try again"
    rm new_hostfile
    rm new_rankfile
    exit 1
  else
    echo "Incorrect input. Try again:"
  fi
done
