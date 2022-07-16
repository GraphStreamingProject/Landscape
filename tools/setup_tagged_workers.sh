#!/bin/bash
set -e
if [[ $# -ne 1 ]]; then
  echo "Invalid arguments. Require num_cpu_per_node"
  echo "num_cpu_per_node:  The number of CPUs on each worker."
  exit
fi

cur_dir=`dirname "$(readlink -f "$0")"`

$cur_dir/fetch_ips.sh > $cur_dir/node_list.txt
$cur_dir/cluster_basic_init.sh $cur_dir/node_list.txt $1

cp $cur_dir/hostfile $cur_dir/..
cp $cur_dir/hostfile $cur_dir/../build
cp $cur_dir/hostfile ~/

ansible-playbook -f 200 -i inventory.ini $cur_dir/ansible/ssh.yaml
ansible-playbook -f 200 -i inventory.ini $cur_dir/ansible/mpi.yaml
ansible-playbook -f 200 -i inventory.ini $cur_dir/ansible/files.yaml
