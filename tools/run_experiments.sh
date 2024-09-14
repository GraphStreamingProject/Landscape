#!/usr/bin/env bash
# Recommended not to run this file directly. It is called by the runme.sh script.


# TODO: Make this code detect and throw failures early so that things don't cascade in weird ways
#       will require this error checking in helper scripts as well

function runcmd {
  echo "Running command $@"
  "$@"
}

get_file_path() {
  # $1 : relative filename
  echo "$(cd "$(dirname "$1")" && pwd)/$(basename "$1")"
}

plotting_dir=$(get_file_path plotting)
csv_directory=$(get_file_path csv_files)
project_dir=$(get_file_path .)

mkdir $csv_directory

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

echo "AWS CLI Configuration"
echo "Enter AWS Access Key + Secret and the region of the main node"
echo "AWS Access Keys can be managed under IAM->Users->YourUsername->Security credentials"
echo "You can safely leave 'default output format' unchanged"
echo ""

# Install and prompt user to configure
runcmd aws configure

region=$(aws configure get region)

main_id=`ec2-metadata -i | awk '{print $NF}'`
main_zone=`ec2-metadata -z | awk '{print $NF}'`

echo "region = $region"
echo "main info = $main_id $main_zone"

echo "Basic EC2 Configuration..."
runcmd bash tools/aws/create_security_group.sh
runcmd bash tools/aws/placement_group.sh
runcmd bash tools/aws/tag_main.sh
worker_create_args=$(bash tools/aws/get_worker_args.sh $region $main_zone)

echo "Installing Packages..."
echo "  general dependencies..."
runcmd sudo yum update -y
runcmd sudo yum install -y htop gcc-c++ jq python3-pip R texlive-latex libcurl-devel openssl-devel harfbuzz-devel fribidi-devel freetype-devel libpng-devel libtiff-devel libjpeg-devel
runcmd pip install ansible
echo "  cmake..."
runcmd wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
runcmd sudo mkdir /opt/cmake
runcmd sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake --skip-license --exclude-subdir
runcmd sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake
runcmd rm cmake-3.23.0-rc2-linux-x86_64.sh


echo "Installing MPI..."
ansible-playbook --connection=local --inventory 127.0.0.1, tools/ansible/mpi.yaml


echo "Installing R..."
runcmd sudo Rscript $plotting_dir/R_scripts/install.R


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
runcmd bash tools/aws/create_storage.sh $main_id $main_zone
echo "  downloading data..."
runcmd aws s3 sync s3://zeppelin-datasets/ /mnt/ssd1 --exclude 'kron_18_stream_binary'


echo "Creating and Initializing Cluster..."
echo "  creating..."
# ASW CLI STUFF HERE
runcmd cd tools
runcmd python3 aws/create_workers.py --num_workers 48 $worker_create_args
runcmd python3 aws/run_first_n_workers.py --num_workers 48
echo "  initializing..."
runcmd yes | bash setup_tagged_workers.sh $region 36 8

echo "Beginning Experiments..."


echo "/-------------------------------------------------\\"
echo "|         RUNNING SCALE EXPERIMENT (1/5)          |"
echo "\\-------------------------------------------------/"
echo "threads, machines, insertion_rate, query_latency, comm_factor" > $csv_directory/scale_experiment.csv
runcmd python3 aws/run_first_n_workers.py --num_workers 1
runcmd yes | bash setup_tagged_workers.sh $region 36 8

runcmd bash scale_experiment.sh $csv_directory/scale_experiment.csv 1 1 1 1

runcmd python3 aws/run_first_n_workers.py --num_workers 8
runcmd yes | bash setup_tagged_workers.sh $region 36 8
runcmd bash scale_experiment.sh $csv_directory/scale_experiment.csv 8 8 8 1

runcmd python3 aws/run_first_n_workers.py --num_workers 32
runcmd yes | bash setup_tagged_workers.sh $region 36 8
runcmd bash scale_experiment.sh $csv_directory/scale_experiment.csv 16 24 8 3

runcmd python3 aws/run_first_n_workers.py --num_workers 48
runcmd yes | bash setup_tagged_workers.sh $region 36 8
runcmd bash scale_experiment.sh $csv_directory/scale_experiment.csv 32 32 8 7
runcmd bash scale_experiment.sh $csv_directory/scale_experiment.csv 40 48 8 11

runcmd python3 aws/run_first_n_workers.py --num_workers 40
runcmd yes | bash setup_tagged_workers.sh $region 36 8

echo "/-------------------------------------------------\\"
echo "|         RUNNING SPEED EXPERIMENT (2/5)          |"
echo "\\-------------------------------------------------/"
echo "dataset, insert_rate, query_latency, network" > $csv_directory/speed_experiment.csv
runcmd bash speed_experiment.sh $csv_directory/speed_experiment.csv 40 11

echo "/-------------------------------------------------\\"
echo "|         RUNNING QUERY EXPERIMENT (3/5)          |"
echo "\\-------------------------------------------------/"
echo "burst, flush_latency, boruvka_latency, query_type" > $csv_directory/query_experiment.csv
runcmd bash query_exp.sh $csv_directory/query_experiment.csv 40

echo "/-------------------------------------------------\\"
echo "|        RUNNING K-SPEED EXPERIMENT (4/5)         |"
echo "\\-------------------------------------------------/"
echo "dataset, k, ins_per_sec, query_latency, memory, network" > $csv_directory/k_speed_experiment.csv
runcmd bash k_speed_experiment.sh $csv_directory/k_speed_experiment.csv 40 7 2
runcmd bash k_speed_experiment.sh $csv_directory/k_speed_experiment.csv 40 7 8

echo "/-------------------------------------------------\\"
echo "|        RUNNING ABLATIVE EXPERIMENT (5/5)        |"
echo "\\-------------------------------------------------/"
echo "threads, workers, ingest_rate, comm_factor, system" > $csv_directory/ablative.csv
runcmd bash ablative_experiment.sh $csv_directory/ablative.csv $region

runcmd python3 aws/run_first_n_workers.py --num_workers 0

runcmd cp $csv_directory/scale_experiment.csv $plotting_dir/R_scripts/scaling_data.csv
runcmd cp $csv_directory/query_experiment.csv $plotting_dir/R_scripts/dsu_query.csv
runcmd cp $csv_directory/ablative.csv $plotting_dir/R_scripts/ablative_scaling_data.csv
runcmd tail -n+2 $csv_directory/scale_experiment.csv > temp_file
runcmd sed -n 's/$/, Cameo + PHT/' temp_file >> $plotting_dir/R_scripts/ablative_scaling_data.csv
runcmd rm temp_file

runcmd cp $csv_directory/speed_experiment.csv $plotting_dir/latex/speed.csv
runcmd cp $csv_directory/k_speed_experiment.csv $plotting_dir/latex/k_speed.csv
runcmd cd $plotting_dir
runcmd ./plot.sh
runcmd cd $project_dir

# TODO: Terminate the cluster
runcmd python3 aws/terminate_workers.py


echo "Experiments are completed."
echo "If you do NOT intend to run the experiments again we recommend deleting the datasets volume."
echo ""
while :
do
  read -r -p "Do you want to delete the datasets volume(Y/N): " delete
  case "$delete" in
    'N'|'n') break;;
    'Y'|'y') bash delete_storage.sh && break;;
  esac
done

