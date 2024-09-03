#!/usr/bin/env bash

function runcmd {
  echo "Running command $@"
  "$@"
}

get_file_path() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

dataset_disk_loc=/mnt/ssd1/
csv_directory=csv_files

datasets=(
  'kron13'
  'kron15'
  'kron16'
  'kron17'
  'p2p-gnutella'
  'rec-amazon'
  'google-plus'
  'web-uk'
)
dataset_sizes=(
  '150.6MB'
  '2.3GB'
  '9.4GB'
  '37.5GB'
  '2.5MB'
  '2.1MB'
  '232.9MB'
  '200.1MB'
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
# Install and prompt user to configure
# TODO: Prompt user or pull from cli config
region=us-west-2

echo "Installing Packages..."
echo "  general dependencies..."
runcmd sudo yum update -y
runcmd sudo yum install -y tmux htop gcc-c++ jq python3-pip
runcmd pip install ansible
echo "  cmake..."
runcmd wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
runcmd sudo mkdir /opt/cmake
runcmd sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake
runcmd sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake


echo "Building Landscape..."
runcmd mkdir -p build
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


echo "Downloading Datasets..."
# AWS CLI STUFF HERE


echo "Creating and Initializing Cluster..."
# ASW CLI STUFF HERE
runcmd cd tools
runcmd tools/setup_tagged_workers.sh $region 36 8
runcmd cd ..

echo "Beginning Experiments..."


exit
