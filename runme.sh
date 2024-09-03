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
wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
sudo mkdir /opt/cmake
sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake
sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake


echo "Downloading Datasets..."
# AWS CLI STUFF HERE

echo "Creating and Initializing Cluster..."
# ASW CLI STUFF HERE
cd tools
tools/setup_tagged_workers.sh $region 36 8
cd ..

echo "Beginning Experiments..."


exit

# This option is ONLY for debugging this script. Causes all experiments to be run only upon kron13
# full_expr=2

corr_idx=(3 5 6 7 8)
declare -a all_idx
declare -a kron_idx
declare -i cont_expr_samples=100
declare -i cont_expr_runs
declare -i aspen_terrace_timeout
if (( $full_expr == 1 )); then
  all_idx=(0 1 2 3 4 5 6 7 8)
  kron_idx=(0 1 2 3 4)
  cont_expr_runs=10
  aspen_terrace_timeout=24
elif (( $full_expr == 2 )); then
  # DEBUGGING option that runs all tests just on kron13
  all_idx=(0)
  kron_idx=(0)
  corr_idx=(0)
  cont_expr_runs=1
  aspen_terrace_timeout=6
else
  all_idx=(0 1 2 3 5 6 7 8)
  kron_idx=(0 1 2 3)
  cont_expr_runs=1
  aspen_terrace_timeout=6
fi

all_datasets=()  # every dataset
kron_datasets=() # datasets for speed experiments (all kron graphs)
corr_datasets=() # datasets for correctness tests

for idx in "${all_idx[@]}"; do
  all_datasets+=(${dataset_filenames[$idx]})
done

for idx in "${kron_idx[@]}"; do
  kron_datasets+=($dataset_disk_loc/${dataset_filenames[$idx]})
done

for idx in "${corr_idx[@]}"; do
  corr_datasets+=($dataset_disk_loc/${dataset_filenames[$idx]})
done

kron_17_dataset="${kron_datasets[3]}"
if (( $full_expr == 2 )); then
  # DEBUGGING -- force to kron13
  kron_17_dataset="${kron_datasets[0]}"
fi

echo 'Running CMake build'

runcmd mkdir -p build
runcmd cd build
runcmd cmake ..
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code from 'cmake ..' when making GraphZeppelin"
  exit
fi
runcmd make -j
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code from 'make -j' when making GraphZeppelin"
  exit
fi

echo 'Finished running CMake build'

echo 'Building Aspen and Terrace!'
cd ../comparison_systems/aspen/code
(export CILK=1 ; make -j ingestion_test continuous_query_test)
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code from 'make -j' when making Aspen"
  exit
fi
cd -

cd ../comparison_systems/terrace/code
(export CILK=1 ; make -j ingestion_test continuous_query_test)
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code from 'make -j' when making Terrace"
  exit
fi
cd -

cp ../comparison_systems/terrace/code/ingestion_test terrace_ingest_expr &&
cp ../comparison_systems/terrace/code/continuous_query_test terrace_query_expr &&
cp ../comparison_systems/aspen/code/ingestion_test aspen_ingest_expr &&
cp ../comparison_systems/aspen/code/continuous_query_test aspen_query_expr
if [[ $? -ne 0 ]]; then
  echo "ERROR: Non-zero exit code when copying Aspen/Terrace executables to build directory"
  exit
fi

echo 'Finished building Aspen and Terrace'

if ! ln -f -s $GZ_disk_loc 'graphzeppelin_disk_link'; then
  echo "ERROR! Could not create symbolic link! GraphZeppelin may not be as efficient without it!"
  echo "Falling back to directory in build"
  mkdir graphzeppelin_disk_link
  sleep 5
fi

echo 'Downloading datasets'

for dataset in "${all_datasets[@]}"; do
  runcmd wget -nc --tries=10 -O "$dataset_disk_loc/$dataset" "$dataset_link_dir/$dataset"
done

echo 'Finished downloading datasets'

echo 'Running experiments'

echo $kron_17_dataset

runcmd mkdir $csv_directory 2> /dev/null

runcmd ../sketch_expr/run_sketch_expr.sh $csv_directory

runcmd ./speed_experiment yes $aspen_terrace_timeout $csv_directory ${kron_datasets[@]}

runcmd ./query_experiment yes $csv_directory $kron_17_dataset $aspen_terrace_timeout

runcmd ../buffer_expr/run_buffer_expr.sh $kron_17_dataset $csv_directory

runcmd ./parallel_experiment $kron_17_dataset delme "$csv_directory/parallel_expr.csv" 46

runcmd ../cont_expr/run_correctness_test.sh $cont_expr_samples $cont_expr_runs $csv_directory ${corr_datasets[@]}

echo 'Finished running experiments'

# Get rid of top configuration and temporary files
rm -f ~/.config/procps/toprc
rm -f delme*
rm -f temp*
rm -f TEMP*
rm -fr speed_results*
rm -fr speed_gutter_results
rm -fr unlim_*

# Plotting code here
echo 'Copying csv data to plotting directory'
cp $csv_directory/parallel_expr.csv ../plotting/R_scripts/parallel_data.csv
cp $csv_directory/buffer_expr.csv ../plotting/R_scripts/parallel_data3.csv
cp $csv_directory/unlim_query_expr.csv ../plotting/R_scripts/query.csv
cp $csv_directory/lim_query_expr.csv ../plotting/R_scripts/query_disk.csv
cp $csv_directory/mem_usage.csv ../plotting/R_scripts/space_data.csv
cp $csv_directory/lim_speed_expr.csv ../plotting/R_scripts/speed_data.csv
cp $csv_directory/unlim_speed_expr.csv ../plotting/R_scripts/speed_data_unlim.csv

cp $csv_directory/correctness.csv ../plotting/latex/correct.csv
cp $csv_directory/sketch_space.csv ../plotting/latex/l0size.csv
cp $csv_directory/sketch_speed.csv ../plotting/latex/l0speed.csv

echo 'Create plots and tables'
cd ../plotting
./plot.sh