#!/bin/bash
set -e
if [[ $# -ne 3 ]]; then
  echo "Invalid arguments. Require region, main_cpu, distrib_cpu"
  echo "region:       The EC2 region where we have machines."
  echo "main_cpu:     The number of physical CPUs on the main node."
  echo "distrib_cpu:  The number of physical CPUs on each DistributedWorker."
  exit
fi

cur_dir=`dirname "$(readlink -f "$0")"`

$cur_dir/fetch_ips.sh $1 > $cur_dir/node_list.txt
$cur_dir/cluster_basic_init.sh $cur_dir/node_list.txt $2 $3

cp $cur_dir/hostfile $cur_dir/../build/
cp $cur_dir/rankfile $cur_dir/../build/

ansible-playbook -f 200 -i inventory.ini $cur_dir/ansible/ssh.yaml
ansible-playbook -f 200 -i inventory.ini $cur_dir/ansible/mpi.yaml
ansible-playbook -f 200 -i inventory.ini $cur_dir/ansible/files.yaml
