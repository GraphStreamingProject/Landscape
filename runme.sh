#!/usr/bin/env bash

function runcmd {
  echo "Running command $@"
  "$@"
}

get_file_path() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

results_directory=`get_file_path results`

datasets=(
  'kron13'
  'kron15'
  'kron16'
  'kron17'
  'ca_citeseer'
  'google-plus'
  'p2p-gnutella'
  'rec-amazon'
  'web-uk'
)
# Size of each dataset in MiB
dataset_sizes=(
  '151'
  '2403'
  '9608'
  '38408'
  '14'
  '233'
  '2.5'
  '2.1'
  '200'
)
dataset_filenames=(
  'kron_13_stream_binary'
  'kron_15_stream_binary'
  'kron_16_stream_binary'
  'kron_17_stream_binary'
  'real_streams/ca_citeseer_stream_binary'
  'real_streams/google_plus_stream_binary'
  'real_streams/p2p_gnutella_stream_binary'
  'real_streams/rec_amazon_stream_binary'
  'real_streams/web_uk_stream_binary'
)
s3_bucket=zeppelin-datasets


echo "=== Landscape Experiments ==="
echo "This script runs the experiments for our paper Landscape. It is designed for Amazon EC2."
echo "It utilizes sudo commands to install packages and configure the system."
echo ""
while :
do
  read -r -p "Do you want to continue(Y/N): " c
  case "$c" in
    'N'|'n') exit;;
    'Y'|'y') break;;
  esac
done

echo "AWS CLI Configuration"
echo "Enter AWS Access Key + Secret and the region of the main node"
echo "AWS Access Keys can be managed under IAM->users->Security credentials"

# Install and prompt user to configure
runcmd aws configure
region=$(aws configure region)

main_meta=`runcmd tools/aws/get_main_metadata`

echo "Installing Packages..."
echo "  general dependencies..."
runcmd sudo yum update -y
runcmd sudo yum install -y tmux htop gcc-c++ jq python3-pip
runcmd pip install ansible
echo "  cmake..."
runcmd wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
runcmd sudo mkdir /opt/cmake
runcmd sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake --skip-license --exclude-subdir
runcmd sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake


echo "Installing MPI..."
ansible-playbook --connection=local --inventory 127.0.0.1, tools/ansible/mpi.yaml


echo "Building Landscape..."
runcmd mkdir -p build
runcmd mkdir -p results
runcmd cd build
runcmd cmake ..
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code from 'cmake ..' when making Landscape"
  exit
fi
runcmd make -j
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code from 'make -j' when making Landscape"
  exit
fi
runcmd cd ..


echo "Get Datasets..." 
echo "  creating EC2 volume..."
runcmd tools/aws/create_storage.sh $main_meta
echo "  downloading data..."
aws s3 cp s3://zeppelin-datasets/kron_1[3-7]* /mnt/ssd1
aws s3 cp s3://zeppelin-datasets/real_streams /mnt/ssd1 --recursive


echo "Creating and Initializing Cluster..."
echo "  creating..."
# ASW CLI STUFF HERE
echo "  initializing..."
runcmd cd tools
runcmd tools/setup_tagged_workers.sh $region 36 8

# TODO: SHUTDOWN ALL BUT 1 WORKER

echo "Beginning Experiments..."


echo "/-------------------------------------------------\\"
echo "|         RUNNING SCALE EXPERIMENT (1/?)          |"
echo "\\-------------------------------------------------/"
runcmd echo "workers, machines, insert_rate, query_latency, comm_factor" > ../results/scale_experiment.csv
runcmd ./scale_experiment 1 1 1 1
# TODO: TURN ON 7 MORE WORKERS
runcmd ./scale_experiment 4 8 4 1
# TODO: TURN ON 24 MORE WORKERS
runcmd ./scale_experiment 16 24 8 3
# TODO: TURN ON 32 MORE WORKERS
runcmd ./scale_experiment 32 32 8 7
runcmd ./scale_experiment 40 64 8 11

# TODO: TURN OFF ALL BUT 40 WORKERS

echo "/-------------------------------------------------\\"
echo "|         RUNNING SPEED EXPERIMENT (2/?)          |"
echo "\\-------------------------------------------------/"
runcmd echo "dataset, insert_rate, query_latency, comm_factor" > ../results/speed_experiment.csv
runcmd ./speed_experiment

echo "/-------------------------------------------------\\"
echo "|         RUNNING QUERY EXPERIMENT (3/?)          |"
echo "\\-------------------------------------------------/"
runcmd echo "TODO" > ../results/query_experiment.csv
runcmd ./query_exp.sh

echo "/-------------------------------------------------\\"
echo "|        RUNNING K-SPEED EXPERIMENT (4/?)         |"
echo "\\-------------------------------------------------/"
runcmd echo "TODO" > ../results/k_speed_experiment.csv
runcmd ./k_speed_experiment.sh

# TODO: Generate figures and tables

# TODO: Terminate the cluster

echo "Experiments are completed."
echo "If you do not intend to run the experiments again we recommend deleting the datasets volume."
echo "Do you want to delete the datasets volume?"
