input_file=$1
num_cpu=$2

if [[ $# -ne 2 ]]; then
  echo "Invalid arguments. Require node_list, num_cpu_per_node"
  echo "Node list:         A file that contains the AWS private DNS addresses with main node first."
  echo "num_cpu_per_node:  The number of CPUs on each worker."
  exit
fi

echo $input_file
echo $num_cpu

first=0 # True

while read line; do
  if [[ $first == 0 ]]; then
    echo "Reading main: $line"
    first=1 # False
    echo "[master]" > new_inventory.ini
    echo "$line" >> new_inventory.ini
    echo "[workers]" >> new_inventory.ini

    new_ip=${line//-/.}
    new_ip=${new_ip/ip./}
    new_ip=${new_ip/.ec2.internal/}
    echo "$new_ip slots=1 max_slots=1" > new_hostfile

  else
    echo "Reading worker: $line"
    echo "$line" >> new_inventory.ini

    new_ip=${line//-/.}
    new_ip=${new_ip/ip./}
    new_ip=${new_ip/.ec2.internal/}
    echo "$new_ip slots=$num_cpu" >> new_hostfile
  fi
  ssh-keyscan -H $line >> ~/.ssh/known_hosts # add other machine's public key to our known_hosts
done <$input_file

cat new_inventory.ini
echo "Is this inventory correct? [y/n]:"
while true; do
  read correct

  if [[ $correct = [yY] ]]; then
    mv new_inventory.ini inventory.ini
    break
  elif [[ $correct = [nN] ]]; then
    echo "Please ensure that the node list is correct and try again"
    rm new_inventory.ini
    exit
  else
    echo "Incorrect input. Try again:"
  fi
done

cat new_hostfile
echo "Is this hostfile correct? [y/n]:"
while true; do
  read correct

  if [[ $correct = [yY] ]]; then
    mv new_hostfile hostfile
    break
  elif [[ $correct = [nN] ]]; then
    echo "Please ensure that the node list is correct and try again"
    rm new_hostfile
    exit
  else
    echo "Incorrect input. Try again:"
  fi
done
