# Landscape
Linear sketching for the connected components and k-edge connectivity problems. Landscape distributes the CPU intensive work of performing sketch updates to many worker nodes while keeping sketch data on the main node. The result is that we can process graph update streams at near sequential RAM bandwidth speeds.

## Using Landscape
Landscape is a c++ library built with cmake. You can easily use Landscape in your code through cmake with FetchContent or ExternalProjectAdd.

## Reproducing Our Experiments
Landscape appears in [ALENEX'25](). You can reproduce our experimental results by following these instructions.



## Cluster Provisioning
### Ensure the master is able to read IPS
There is an IAM Role that allows the EC2 instance to read IPS. This is used to automatically get the IPS.
![image](https://user-images.githubusercontent.com/4708326/164508403-70fbb271-fa4c-4145-9093-ff86320e1bba.png)

### Tag the Master and Worker nodes

The script only reads properly tagged EC2 instances. The Master must be tagged 'ClusterNodeType:Master' to appear at the top of the host files. The Workers must be tagged 'ClusterNodeType:Worker'.

![image](https://user-images.githubusercontent.com/4708326/164511717-02f2feee-a9f8-4b04-a35e-fb53be5140ee.png)

## Main Node Installation and Setup

### 1. Install packages
```
sudo yum update -y
sudo yum install -y tmux htop git gcc-c++ jq python3-pip
pip install ansible
```

### 2. Install cmake version 3.16+
First Step:
#### x86_64
```
wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc2/cmake-3.23.0-rc2-linux-x86_64.sh
sudo mkdir /opt/cmake
sudo sh cmake-3.23.0-rc2-linux-x86_64.sh --prefix=/opt/cmake
```
#### aarch64
```
wget https://github.com/Kitware/CMake/releases/download/v3.23.0-rc5/cmake-3.23.0-rc5-linux-aarch64.sh
sudo mkdir /opt/cmake
sudo sh cmake-3.23.0-rc5-linux-aarch64.sh --prefix=/opt/cmake
```
Second Step:
```
sudo ln -s /opt/cmake/bin/cmake /usr/local/bin/cmake
```
When running cmake .sh script enter y to license and n to install location.  
These commands install cmake version 3.23 but any version >= 3.16 will work.

### 4. Setup ssh keys
* Copy EMR.pem to cluster `rsync -ve "ssh -i </path/to/EMR.pem>" </path/to/EMR.pem> <AWS-user>@<main_node_dns_addr>:.`
* Ensure key being used is default rsa key for ssh `id_rsa` for example `cp EMR.pem ~/.ssh/id_rsa`

### 5. Clone DistributedStreamingCC Repo

## Cluster Setup

### Run `setup_tagged_workers.sh`  
This bash script will construct the ansible `inventory.ini` file; and the MPI `hostfile` and `rankfile`. The arguments to the script are the EC2 region where our machines are, number of physical CPUs on the main node, and number of physical CPUs on the worker nodes.

Example:
```
./setup_tagged_workers.sh us-west-2 36 8
```
The script will automatically set the known_hosts for all the machines in the cluster to whatever ssh-keyscan finds (this is a slight security issue if you don't trust the cluster but should be fine as we aren't transmitting sensative data). It will additionally confirm with you that the `inventory.ini` and `hostfile` it creates look reasonable.

### Run unit tests
After running the setup script you should be able to run the unit tests from the build directory.
```
mpirun -np 22 -hostfile hostfile -rf rankfile ./distrib_tests
```
-np denotes the number of processes to run. Should be number of worker nodes +21.

## Cluster Setup (Manual)
Ansible files for setting up the cluster are found under `tools/ansible`.  
Ansible commands are run with `ansible-playbook -i /path/to/inventory.ini /path/to/<script>.yaml`.

### 1. Distribute ssh keys to cluster
* Run ansible file `ssh.yaml` with `ansible-playbook -i inventory.ini DistributedStreamingCC/tools/ansible/ssh.yaml`

### 2. Install MPI on nodes in cluster
* Run ansible file `mpi.yaml` with `ansible-playbook -i inventory.ini DistributedStreamingCC/tools/ansible/mpi.yaml`
* Run `source ~/.bashrc` in open terminal on main node

### 3. Build Distributed Streaming Repo
* make `build` directory in project repo
* run `cmake .. ; make -j` in build directory

### 4. Distribute executables and hostfile to worker nodes
*  Run ansible file `files.yaml` with `ansible-playbook -i inventory.ini DistributedStreamingCC/tools/ansible/files.yaml`

### EFA Installation instructions
* Follow the instructions at https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/efa-start.html#efa-start-security

## Amazon Storage
### EBS Storage
EBS disks are generally found installed at `/mnt/nvmeXnX` where X is the disk number. In order to use them, the disk must be formatted and then mounted.
* `sudo lsblk -f` to list all devices
* (Optional) If no filesystem exists on the device than run `sudo mkfs -t xfs /dev/<device>` to format the drive. This will overwrite the content on the device so DO NOT do this if a filesystem already exists.
* Create the mount point directory `sudo mkdir /mnt/<mnt_point>`
* Mount the device `sudo mount /dev/<device> /mnt/<mnt_point>`
* Adjust owner and permissions of mount point `sudo chown -R <user> /mnt/<mnt_point>` and `chmod a+rw /mnt/<mnt_point>` 

## Single Machine Setup

### 1. Install OpenMPI
For Ubuntu the following will install openmpi
```
sudo apt update
sudo apt install libopenmpi-dev
```
Google is your friend for other operating systems :)

### 2. Run executables
Use the `mpirun` command to run mpi programs. For example, to run the unit tests with 4 processes, the following command is used.
```
mpirun -np 4 ./distrib_tests
```

## Tips for Debugging with MPI
If you want to run the code using a debugging tool like gdb you can perform the following steps.
1. Compile with debugging flags `cmake -DCMAKE_BUILD_TYPE=Debug .. ; make`
2. Launch the mpi task with each process in its own window using xterm `mpirun -np <num_proc> term -hold -e gdb <executable>`

Print statement debugging can also be helpful, as even when running in a cluster across many machines, all the output to console across the workers is printed out by the main process. 
